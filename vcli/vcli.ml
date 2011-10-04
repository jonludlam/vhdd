open Smapi_types
open Stringext
open Listext

module SC=Smapi_client

exception Vcli_failure of string

(** Parse a string which might have a units suffix on the end *)
let bytes_of_string field x = 
  let isdigit c = c >= '0' && c <= '9' in
  let ( ** ) a b = Int64.mul a b in
  let max_size_TiB = Int64.div Int64.max_int (1024L ** 1024L ** 1024L ** 1024L) in
  (* detect big number that cannot be represented by Int64. *)
  let int64_of_string s =
    try
      Int64.of_string s
    with _ ->
      if s = "" then
        raise (Vcli_failure (Printf.sprintf "Failed to parse field '%s': expecting an integer (possibly with suffix)" field));
      let alldigit = ref true and i = ref (String.length s - 1) in
      while !alldigit && !i > 0 do alldigit := isdigit s.[!i]; decr i done;
      if !alldigit then
        raise (Vcli_failure (Printf.sprintf "Failed to parse field '%s': number too big (maximum = %Ld TiB)" field max_size_TiB))
      else
        raise (Vcli_failure (Printf.sprintf "Failed to parse field '%s': expecting an integer (possibly with suffix)" field));
    in
  match (String.split_f (fun c -> String.isspace c || (isdigit c)) x) with
  | [] -> 
      (* no suffix on the end *)
      int64_of_string x
  | [ suffix ] -> begin
	let number = match (String.split_f (fun x -> not (isdigit x)) x) with
	  | [ number ] -> int64_of_string number
	  | _ -> raise (Vcli_failure (Printf.sprintf "Failed to parse field '%s': expecting an integer (possibly with suffix)" field)) in
	let multiplier = match suffix with
	  | "bytes" -> 1L
	  | "KiB" -> 1024L
	  | "MiB" -> 1024L ** 1024L
	  | "GiB" -> 1024L ** 1024L ** 1024L 
	  | "TiB" -> 1024L ** 1024L ** 1024L ** 1024L
	  | x -> raise (Vcli_failure (Printf.sprintf "Failed to parse field '%s': Unknown suffix: '%s' (try KiB, MiB, GiB or TiB)" field x)) in
        (* FIXME: detect overflow *)
	number ** multiplier
    end
  | _ -> raise (Vcli_failure (Printf.sprintf "Failed to parse field '%s': expecting an integer (possibly with suffix)" field))


let rpc host port path xml = 
  let open Xmlrpc_client in
      XML_protocol.rpc ~transport:(TCP (host,port)) ~http:(xmlrpc ~version:"1.0" "path") xml

let remote_rpc task_id host port =
  Int_rpc.wrap_rpc (Vhdrpc.remote_rpc task_id host port)

exception Missing_param of string

let get_param params param = 
  try List.assoc param params with Not_found -> raise (Missing_param param)

let get_map stem' params =
  let stem = Printf.sprintf "%s:" stem' in
  let stemlen = String.length stem in
  let prefixed_map_params = List.filter (fun (key,value) -> String.startswith stem key) params in
  let map_params = List.map (fun (key,value) -> (String.sub key stemlen (String.length key - stemlen), value)) prefixed_map_params in
  map_params

let get_gp params =
  let device_config = get_map "device-config" params in
  let sr_sm_config = get_map "sr-sm-config" params in
  {gp_device_config=device_config; gp_sr_sm_config=sr_sm_config; gp_xapi_params=None}

let get_sr params = 
  Some {sr_uuid=get_param params "sr"}

let get_vdi params =
  get_param params "location"

let sr_create rpc params =
  let gp = get_gp params in
  let sr = get_sr params in
  let size = try Int64.of_string (get_param params "size") with _ -> 0L in
  SC.SR.create rpc gp sr size

let sr_delete rpc params =
  let gp = get_gp params in
  let sr = get_sr params in
  SC.SR.delete rpc gp sr
  
let sr_attach rpc params =
  let gp = get_gp params in
  let sr = get_sr params in
  SC.SR.attach rpc gp sr

let sr_detach rpc params =
  let gp = get_gp params in
  let sr = get_sr params in
  SC.SR.detach rpc gp sr

let sr_probe rpc params =
  let gp = get_gp params in
  let sr_sm_config = get_map "sr-sm-config" params in
  let result = SC.SR.probe rpc gp sr_sm_config in
  Printf.printf "%s\n" result

let sr_scan rpc params =
  let gp = get_gp params in
  let sr = get_sr params in
  let result = SC.SR.scan rpc gp sr in
  Printf.printf "%s\n" result

let vdi_create rpc params = 
  let gp = get_gp params in
  let sr = get_sr params in
  let sm_config = get_map "sm-config" params in
  let size = bytes_of_string "size" (get_param params "size") in
  let vdi = SC.VDI.create rpc gp sr sm_config size in
  Printf.printf "%s\n" vdi.vdi_location

let vdi_attach rpc params =
  let gp = get_gp params in
  let sr = get_sr params in
  let vdi = get_vdi params in
  let writable = try bool_of_string (get_param params "writable") with Missing_param _ -> true in
  let s = SC.VDI.attach rpc gp sr vdi writable in
  Printf.printf "%s\n" s

let vdi_delete rpc params =
  let gp = get_gp params in
  let sr = get_sr params in
  let vdi = get_vdi params in
  SC.VDI.delete rpc gp sr vdi

let vdi_activate rpc params =
  let gp = get_gp params in
  let sr = get_sr params in
  let vdi = get_vdi params in
  SC.VDI.activate rpc gp sr vdi

let vdi_deactivate rpc params =
  let gp = get_gp params in
  let sr = get_sr params in
  let vdi = get_vdi params in
  SC.VDI.deactivate rpc gp sr vdi

let vdi_detach rpc params =
  let gp = get_gp params in
  let sr = get_sr params in
  let vdi = get_vdi params in
  SC.VDI.detach rpc gp sr vdi

let vdi_clone rpc params =
  let gp = get_gp params in
  let sr = get_sr params in
  let vdi = get_vdi params in
  let driver_params = get_map "driver-params" params in
  let vdi = SC.VDI.clone rpc gp sr driver_params vdi in
  Printf.printf "%s\n" vdi.vdi_location

let vdi_snapshot rpc params =
  let gp = get_gp params in
  let sr = get_sr params in
  let vdi = get_vdi params in
  let driver_params = get_map "driver-params" params in
  let vdi = SC.VDI.snapshot rpc gp sr driver_params vdi in
  Printf.printf "%s\n" vdi.vdi_location

let vdi_generate_config rpc params =
    let gp = get_gp params in
	let sr = get_sr params in
	let vdi = get_vdi params in
	let result = SC.VDI.generate_config rpc gp sr vdi in
	Printf.printf "%s\n" result

let vdi_attach_from_config rpc params =
	let config_file = get_param params "config-file" in
	let config = Unixext.string_of_file config_file in
	let result = SC.VDI.attach_from_config rpc config in
	Printf.printf "%s\n" result

let host_set_dead intrpc params =
  let host_uuid = get_param params "host-uuid" in
  Int_client.Host.set_dead intrpc host_uuid

let rolling_upgrade_finished intrpc params =
	Int_client.Host.rolling_upgrade_finished intrpc 

let waiting_mode_set intrpc params =
  let mode = get_param params "mode" in
    Int_client.Debug.waiting_mode_set intrpc (bool_of_string mode)

let waiting_locks_get intrpc =
  let locks = Int_client.Debug.waiting_locks_get intrpc in
    List.iter (fun (lock,lcontext) ->
		 Printf.printf "%s: lock_required: %s reason: %s\n" lock lcontext.Int_types.lc_lock_required lcontext.Int_types.lc_reason) locks

let waiting_lock_unwait intrpc params =
  let lock = get_param params "lock" in
    Int_client.Debug.waiting_lock_unwait intrpc lock

let die intrpc params =
  Int_client.Debug.die intrpc (List.mem_assoc "restart" params)

let _ =
  (*  try*)
  let args = List.tl (Array.to_list Sys.argv) in
  let params = List.filter_map 
	  (fun arg -> 
		  match String.split '=' arg with 
			  | [a;b] -> 
				  Some (a,b) 
			  | _ -> None) args in
  
  let cmd = get_param params "cmd" in
  let host = try List.assoc "host" params with _ -> "localhost" in
  let port = try int_of_string (List.assoc "port" params) with _ -> 4094  in
  let rpc xml = 
    let sm = try List.assoc "smpath" params with _ -> "lvmnew" in 
    rpc host port (Printf.sprintf "/%s" sm) xml
  in
  let intrpc =  remote_rpc "dummy" host port in
  match cmd with
    | "sr-create" -> sr_create rpc params
    | "sr-attach" -> sr_attach rpc params
    | "sr-detach" -> sr_detach rpc params
    | "sr-probe" -> sr_probe rpc params
	| "sr-scan" -> sr_scan rpc params
    | "vdi-create" -> vdi_create rpc params
	| "vdi-delete" -> vdi_delete rpc params
    | "vdi-attach" -> vdi_attach rpc params
    | "vdi-activate" -> vdi_activate rpc params
    | "vdi-detach" -> vdi_detach rpc params
    | "vdi-deactivate" -> vdi_deactivate rpc params
    | "vdi-clone" -> vdi_clone rpc params
    | "vdi-snapshot" -> vdi_snapshot rpc params
	| "vdi-generate-config" -> vdi_generate_config rpc params
    | "host-set-dead" -> host_set_dead intrpc params
	| "rolling-upgrade-finished" -> rolling_upgrade_finished intrpc params
	| "die" -> die intrpc params
	| "waiting-mode-set" -> waiting_mode_set intrpc params
	| "waiting-locks-get" -> waiting_locks_get intrpc
	| "waiting-lock-unwait" -> waiting_lock_unwait intrpc params
	| _ ->
		Printf.fprintf stderr "Unknown command %s" cmd;
		exit 1
		

	(*
  with _ -> 
    
      | "vdi-clone" -> vdi_clone params

      | *)
