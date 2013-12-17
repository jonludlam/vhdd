(* Circular buffer metadata writing *)

(* Write a string to a circular buffer with a header pointing at the current version *)

(* On disk layout: 4096 byte reserved for header, then the circular buffer starts
   Header area consists of:

   1 byte version, encoded in ascii
   12 bytes ascii decimal encoded length of json-encoded header (with leading zeros)
   json-encoded header
*)

module D = Debug.Make(struct let name="circ" end)
open D

exception No_data
exception Corrupt_data
exception Error_writing (* Raised when there's an error in writing the header or data *)
exception Out_of_space (* Cannot fit this and the previous data in the circular buffer concurrently *)
exception Unsupported_version of char

let header_reserved_size = 4096
let header_max_size = 512
let version_size = 1
let header_length_size = 12 (* nb, this is explicitly written in the format string below *)
let digest_size = 16

let current_version='1'

type header = {
	data_len : int;
	data_offset : int; (* from start of circular buffer *)
	data_checksum : string;
	size : int; (* total size, including the header area *)
} with rpc

let ioretry_period = 1.0
let ioretry_max = 20

let apply_with_retry f =
	let rec loop n =
		try
			f ()
		with
			| Unix.Unix_error(Unix.EIO,_,_)
			| Unix.Unix_error(Unix.EAGAIN,_,_) as e ->
				Thread.delay ioretry_period;
				if n <= 0 then raise e else loop (n-1)
	in loop ioretry_max

let rec really_read fd string off n =
	if n=0 then () else
		let m = apply_with_retry (fun () -> Unix.read fd string off n) in
		if m=0 then raise End_of_file;
		really_read fd string (off+m) (n-m)

let read_header fd =
	ignore(Unix.lseek fd 0 Unix.SEEK_SET);
	let str = String.make header_max_size '\000' in
	really_read fd str 0 header_max_size;
	let digest = String.sub str 0 digest_size in
	let to_digest = String.sub str digest_size (header_max_size - digest_size) in
	if digest <> Digest.string to_digest then raise Corrupt_data;
	let version = to_digest.[0] in
	if version <> current_version then raise (Unsupported_version version);
	let data_len = int_of_string (String.sub to_digest 1 12) in
	let json = String.sub to_digest 13 data_len in
	header_of_rpc (Jsonrpc.of_string json)

let write_header fd h =
	let str = Jsonrpc.to_string (rpc_of_header h) in
	let len = String.length str in

	let data = Printf.sprintf "%c%012d%s" current_version len str in
	let data_len = String.length data in

	let to_digest = Printf.sprintf "%s%s" data (String.make (header_max_size - digest_size - data_len) '\000') in
	assert(String.length to_digest = header_max_size - digest_size);

	let digest = Digest.string to_digest in
	let to_write = Printf.sprintf "%s%s" digest to_digest in
	assert(String.length to_write = header_max_size);

	ignore(Unix.lseek fd 0 Unix.SEEK_SET);
	let write_len = header_max_size in
	(*  debug "Trying to write '%s'" to_write;*)
	let written_len = apply_with_retry (fun () -> Unix.single_write fd to_write 0 write_len) in
	if write_len <> written_len then begin
		debug "Error in writing during write_header";
		debug "Encoded header was: %s" to_write;
		raise Error_writing
	end

let write path str =
	let fd = Unix.openfile path [Unix.O_RDWR] 0o000 in
	Pervasiveext.finally
		(fun () ->
			let header = read_header fd in
			if (header.data_len + String.length str) > (header.size - header_reserved_size) then raise Out_of_space;

			let new_start = header.data_offset + header.data_len in
			let real_new_offset = new_start mod (header.size - header_reserved_size) in
			let seek_to = real_new_offset + header_reserved_size in
			let data_len = String.length str in
			if seek_to + data_len > header.size then begin
				let first_len = header.size - seek_to in
				let second_len = data_len - first_len in
				ignore(Unix.lseek fd seek_to Unix.SEEK_SET);
				Unixext.really_write_string fd (String.sub str 0 first_len);
				ignore(Unix.lseek fd header_reserved_size Unix.SEEK_SET);
				Unixext.really_write_string fd (String.sub str first_len second_len)
			end else begin
				ignore(Unix.lseek fd seek_to Unix.SEEK_SET);
				Unixext.really_write_string fd (String.sub str 0 data_len)
			end;
			let new_header = {header with data_checksum=Digest.string str; data_len=data_len; data_offset=real_new_offset;} in
			write_header fd new_header
		)
		(fun () -> Unix.close fd)

let read path =
	let fd = Unix.openfile path [Unix.O_RDONLY] 0o000 in
	Pervasiveext.finally
		(fun () ->
			let header = read_header fd in
			if header.data_len = 0 then raise No_data;
			let offset = header_reserved_size + header.data_offset in
			let data =
				if offset + header.data_len > header.size then begin
					let first_len = header.size - offset in
					let second_len = header.data_len - first_len in
					ignore(Unix.lseek fd offset Unix.SEEK_SET);
					let first_part = Unixext.really_read_string fd first_len in
					ignore(Unix.lseek fd header_reserved_size Unix.SEEK_SET);
					let second_part = Unixext.really_read_string fd second_len in
					first_part ^ second_part
				end else begin
					ignore(Unix.lseek fd offset Unix.SEEK_SET);
					Unixext.really_read_string fd header.data_len
				end
			in
			let digest = Digest.string data in
			if digest <> header.data_checksum
			then raise Corrupt_data
			else data )
		(fun () -> Unix.close fd)

(* size includes the header area *)
let init path size =
	let fd = Unix.openfile path [Unix.O_WRONLY; Unix.O_CREAT] 0o000 in
	Pervasiveext.finally
		(fun () ->
			let header = { data_len = 0; data_offset = 0; data_checksum=Digest.string ""; size=size } in
			write_header fd header)
		(fun () ->
			Unix.close fd)
