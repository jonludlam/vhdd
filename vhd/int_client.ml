(* Minimal client for inter-daemon RPCs *)

open Smapi_types
open Int_types
open Vhd_types
open Int_rpc

module D=Debug.Debugger(struct let name="int_client" end)
open D

let process response =
	match response with
		| Success r ->
			r
		| Failure(code,strings) ->
			raise (IntError(code,strings))

let rtte call response =
	debug "Got an unexpected response from call='%s'" (Jsonrpc.to_string (rpc_of_intrpc call));
	debug "Response was: '%s'" (Jsonrpc.to_string (rpc_of_intrpc_response response));
	failwith "Unexpected response"
		
module SR = struct
	let slave_attach rpc token sr_uuid host ids =
		let call = Sr_slave_attach (token,sr_uuid,host,ids) in
		match process (rpc call) with
			| String s -> s
			| x -> rtte call x

	let slave_detach rpc token sr_uuid host =
		let call = Sr_slave_detach (token,sr_uuid,host) in
		match process (rpc call) with
			| Nil -> ()
			| x -> rtte call x

	let slave_recover rpc token sr_uuid master =
		let call = Sr_slave_recover (token, sr_uuid, master) in
		match process (rpc call) with
			| Nil -> ()
			| x -> rtte call x

	let mode rpc sr_uuid =
		let call = Sr_mode sr_uuid in
		match process (rpc call) with
			| Sr_mode_response mode -> mode
			| x -> rtte call x

	let thin_provision_check rpc sr_uuid =
		let call = Sr_thin_provision_check sr_uuid in
		match process (rpc call) with
			| Nil -> ()
			| x -> rtte call x
end

module Vdi = struct
	let slave_attach rpc host_uuid sr_uuid id writable is_reattach =
		let call = Vdi_slave_attach (host_uuid, sr_uuid, id, writable, is_reattach) in
		match process (rpc call) with
			| Slave_attach_info x ->  if !Global.dummy then { x with sa_leaf_path = Printf.sprintf "%s/%s" (Global.get_host_local_dummydir ()) x.sa_leaf_path} else x
			| x -> rtte call x

	let get_slave_attach_info rpc sr_uuid id =
		let call = Vdi_get_slave_attach_info (sr_uuid, id) in
		match process (rpc call) with
			| Slave_attach_info x ->  if !Global.dummy then { x with sa_leaf_path = Printf.sprintf "%s/%s" (Global.get_host_local_dummydir ()) x.sa_leaf_path} else x
			| x -> rtte call x

	let slave_detach rpc host_uuid sr_uuid id =
		let call = Vdi_slave_detach (host_uuid, sr_uuid, id) in
		match process (rpc call) with
			| Nil -> ()
			| x -> rtte call x

	let slave_activate rpc host_uuid sr_uuid id is_reactivate =
		let call = Vdi_slave_activate (host_uuid, sr_uuid, id, is_reactivate) in
		match process (rpc call) with
			| Nil -> ()
			| x -> rtte call x

	let slave_deactivate rpc host_uuid sr_uuid id =
		let call = Vdi_slave_deactivate (host_uuid, sr_uuid, id) in
		match process (rpc call) with
			| Nil -> ()
			| x -> rtte call x

	let slave_reload rpc host sr_uuid ids =
		let call = Vdi_slave_reload (sr_uuid, ids) in
		match process (rpc call) with
			| Nil -> ()
			| x -> rtte call x

	let slave_leaf_coalesce_stop_and_copy rpc host sr_uuid id leaf_path new_leaf_path =
		let call = Vdi_slave_leaf_coalesce_stop_and_copy (sr_uuid, id, leaf_path, new_leaf_path) in
		match process (rpc call) with
			| Nil -> ()
			| x -> rtte call x

	let slave_set_phys_size rpc sr_uuid id size =
		let call = Vdi_slave_set_phys_size (sr_uuid, id, size) in
		match process (rpc call) with
			| Nil -> ()
			| x -> rtte call x

	let thin_provision_request_more_space rpc sr_uuid host dmnaps =
		failwith "Unimplemented"
(*		let args = XMLRPC.To.structure
			[ "sr_uuid", XMLRPC.To.string sr_uuid;
			"host", XMLRPC.To.string host;
			"dmnaps", marshal_dm_names_and_physsizes dmnaps ]
		in
		let methodcall = XMLRPC.To.methodCall "vdi_thin_provision_request_more_space" [args] in
		process from_lvs (methodResponse (rpc methodcall))*)
end

module Host = struct
	
	let set_dead rpc uuid =
		let call = Host_set_dead uuid in
		match process (rpc call) with
			| Nil -> ()
			| x -> rtte call x

	let rolling_upgrade_finished rpc =
		match process (rpc Host_rolling_upgrade_finished) with
			| Nil -> ()
			| x -> rtte Host_rolling_upgrade_finished x
end

(* DEBUG CALLS *)
module Debug = struct
	let waiting_locks_get rpc =
		let call = Debug_waiting_locks_get in
		match process (rpc call) with 
			| Waiting_locks x -> x
			| x -> rtte call x

	let waiting_lock_unwait rpc lock =
		let call = Debug_waiting_lock_unwait lock in
		match process (rpc call) with
			| Nil -> ()
			| x -> rtte call x

	let waiting_mode_set rpc mode =
		let call = Debug_waiting_mode_set mode in
		match process (rpc call) with
			| Nil -> ()
			| x -> rtte call x

	let vdi_get_leaf_path rpc sr_uuid id =
		let call = Debug_vdi_get_leaf_path (sr_uuid, id) in
		match process (rpc call) with
			| String s -> s
			| x -> rtte call x

	let die rpc b =
	  let call = Debug_die b in
	    match process (rpc call) with
	      | Nil -> ()
	      | x -> rtte call x

	let get_id_to_leaf_map rpc sr_uuid =
		let call = Debug_get_id_to_leaf_map sr_uuid in
		match process (rpc call) with
			| Id_to_leaf_map s -> Vhd_types.id_to_leaf_mapping_t_of_rpc s
			| x -> rtte call x

	let get_vhds rpc sr_uuid =
		let call = Debug_get_vhds sr_uuid in
		match process (rpc call) with
			| Vhds s -> Vhd_records.vhd_record_container_of_rpc s
			| x -> rtte call x		

	let get_master_metadata rpc sr_uuid =
		let call = Debug_get_master_metadata sr_uuid in
		match process (rpc call) with
			| Master_metadata s -> Vhd_types.master_sr_metadata_data_of_rpc s
			| x -> rtte call x		

	let get_slave_metadata rpc sr_uuid =
		let call = Debug_get_slave_metadata sr_uuid in
		match process (rpc call) with
			| Slave_metadata s -> Vhd_types.slave_sr_metadata_data_of_rpc s
			| x -> rtte call x		

	let get_attached_vdis rpc sr_uuid =
		let call = Debug_get_attached_vdis sr_uuid in
		match process (rpc call) with
			| Attached_vdis s -> Vhd_types.attached_vdis_t_of_rpc s
			| x -> rtte call x		

	let get_vhd_container rpc sr_uuid =
		let call = Debug_get_vhd_container sr_uuid in
		match process (rpc call) with
			| Vhd_container s -> Lvmabs_types.container_info_of_rpc s
			| x -> rtte call x
				  
	let get_pid rpc =
		let call = Debug_get_pid in
		match process (rpc call) with
			| Vhdd_pid p -> p
			| x -> rtte call x

	let get_attach_finished rpc sr_uuid =
		let call = Debug_get_attach_finished sr_uuid in
		match process (rpc call) with
			| Attach_finished b -> b
			| x -> rtte call x
				  
	let slave_get_leaf_vhduid rpc sr_uuid id =
		let call = Debug_slave_get_leaf_vhduid (sr_uuid, id) in
		match process (rpc call) with
			| Vhduid v -> v
			|  x -> rtte call x

	let write_junk rpc sr_uuid id size n current_junk =
		let call = Debug_write_junk (sr_uuid, id, size, n, current_junk) in
		match process (rpc call) with
			| Junk j -> j
			| x -> rtte call x

	let check_junk rpc sr_uuid id current_junk =
		let call = Debug_check_junk (sr_uuid, id, current_junk) in
		match process (rpc call) with
			| Nil -> ()
			| x -> rtte call x

	let get_host rpc =
		let call = Debug_get_host in
		match process (rpc call) with
			| Host h -> h
			| x -> rtte call x

	let get_ready rpc =
		let call = Debug_get_ready in
		match process (rpc call) with
			| Nil -> ()
			| x -> rtte call x
end
