open Vhd_types
open Threadext
open Stringext
open Int_types

module D=Debug.Make(struct let name="attachments" end)
open D

let get_log_file () = Printf.sprintf "%s/var/run/vhdd/attachments.json" (Global.get_host_local_dummydir ())

type sr_info = {
	drivertype : string;
	path : string;
	uuid : string;
	mode : attach_mode;
	device_config : (string * string) list;
} with rpc

type sr_info_list = sr_info list with rpc

(** Attached list *)
let mutex = Mutex.create ()
let attached_as_master : (string, master_sr_metadata) Hashtbl.t = Hashtbl.create 10
let attached_as_slave : (string, slave_sr_metadata) Hashtbl.t = Hashtbl.create 10

let read_attachments () =
	let a =
		try
			Unixext.string_of_file (get_log_file ())
		with
			| Unix.Unix_error (Unix.ENOENT, _, _) ->
				debug "Ignoring ENOENT while reading attachments file";
				"[]"
	in
	sr_info_list_of_rpc (Jsonrpc.of_string a)

let mutate_attachment_file f =
	Mutex.execute mutex (fun () -> 
		let srs = read_attachments () in
		let srs = f srs in
		let str = Jsonrpc.to_string (rpc_of_sr_info_list srs) in
		let log_file = get_log_file () in
		Unixext.mkdir_rec (Filename.dirname log_file) 0o755;
		Unixext.write_string_to_file log_file str)

let log_attachment sr_info =
	mutate_attachment_file (fun srs ->
		let srs = List.filter (fun sr -> sr.uuid<>sr_info.uuid) srs in
		sr_info::srs)

let log_detachment sr_uuid =
	mutate_attachment_file (fun srs ->
		List.filter (fun sr -> sr.uuid<>sr_uuid) srs)

let log_attachment_new_master sr_uuid master =
	mutate_attachment_file (fun srs ->
		let mysr = List.find (fun sr -> sr.uuid=sr_uuid) srs in
		let others = List.filter (fun sr -> sr.uuid <> sr_uuid) srs in
		{mysr with mode = Slave master}::others)



let attach_as_master sr_uuid metadata = Mutex.execute mutex (fun () -> Hashtbl.replace attached_as_master sr_uuid metadata)
let attach_as_slave sr_uuid metadata = Mutex.execute mutex (fun () -> debug "attach as slave: %s" sr_uuid; Hashtbl.replace attached_as_slave sr_uuid metadata)
let detach_as_master sr_uuid = Mutex.execute mutex (fun () -> Hashtbl.remove attached_as_master sr_uuid)
let detach_as_slave sr_uuid = Mutex.execute mutex (fun () -> Hashtbl.remove attached_as_slave sr_uuid)

(** Get master metadata *)
let gmm sr =
	Mutex.execute mutex (fun () -> try Hashtbl.find attached_as_master sr with Not_found -> failwith "Not attached")

(** Get slave metadata *)
let gsm sr =
	Mutex.execute mutex (fun () -> try Hashtbl.find attached_as_slave sr with Not_found -> failwith "Not attached")

(** Map over metadatas. Hold the lock while doing so... *)
let map hashtbl fn =
	Mutex.execute mutex (fun () -> Hashtbl.fold (fun k v acc -> (fn k v)::acc) hashtbl [])

let map_master_srs f = map attached_as_master f
let map_slave_srs f = map attached_as_slave f
