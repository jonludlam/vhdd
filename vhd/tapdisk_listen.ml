(* Thread to listen to the socket that tapdisk uses for comms *)

open Stringext

module D=Debug.Make(struct let name="tapdisk_listen" end)
open D

let bind () =
	let path = if !Global.dummy then (Printf.sprintf "%s/tapdisk_sock" !Global.dummydir) else "/var/run/vhdd/tapdisk_sock" in
	Unixext.mkdir_safe (Filename.dirname path) 0o700;
	Unixext.unlink_safe path;
	let sockaddr = Unix.ADDR_UNIX(path) in
	let domain = Unix.PF_UNIX in
	let sock = Unix.socket domain Unix.SOCK_STREAM 0 in
	try
		Unix.set_close_on_exec sock;
		Unix.setsockopt sock Unix.SO_REUSEADDR true;
		Unix.bind sock sockaddr;
		Unix.listen sock 128;
		sock
	with e ->
		debug "Caught exception opening tapdisk_listen socket";
		Unix.close sock;
		raise e

let handle_connection sockaddr fd =
	debug "Tapdisk_listen handler called";
	let text = String.create 256 in
	while true do
		try
			Unixext.really_read fd text 0 256;
			let stripped = String.strip (function '\000' -> true | c -> String.isspace c) text in
			debug "Read: %s" stripped;
			match String.split ',' stripped with
				| [dev;size] ->
					let basename = Filename.basename dev in
					let size = Int64.of_string size in
					begin
						match String.split '_' basename with
							| [host_uuid;sr_uuid;id] ->
								(* The 'size' reported is the start of the 2 Meg chunk that's
								   just been allocated in sectors. Need to add 2 megs, then 4k (bitmap of
								   512 bytes plus round up to next 4k page, then add 512 for
								   footer *)
								let start_of_chunk_in_bytes = Int64.mul 512L size in
								let end_of_chunk_in_bytes = Int64.add start_of_chunk_in_bytes 2097152L in
								let end_of_chunk_including_bitmap = Int64.add end_of_chunk_in_bytes 4096L in
								let with_footer = Int64.add end_of_chunk_including_bitmap 512L in
								Int_client.Vdi.slave_set_phys_size (Int_rpc.wrap_rpc ((!Vhdrpc.local_rpc) (Uuidm.to_string (Uuidm.create Uuidm.(`V4))))) sr_uuid id with_footer;
								debug "Done";
							| _ -> debug "Couldn't parse tapdisk device: %s" basename
					end
				| _ -> debug "Protocol failure: got: %s" text
		with e ->
			log_backtrace ();
			debug "Ack! caught exception: %s" (Printexc.to_string e);
			Unix.close fd;
			raise e
	done

let start () =
	let sock = bind () in
	let handler = { Server_io.name="tapdisk_listen";
	body = handle_connection } in
	let _ = Server_io.server handler sock in
	()
