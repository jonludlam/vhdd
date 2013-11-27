(* Internal handler *)
open Int_rpc
open Int_types
open Threadext

module D=Debug.Make(struct let name="int_server" end)
open D

let process ctx call =
	try
		Success
			(match call with
				| Sr_slave_attach (tok, sr_uuid, host, ids) ->
					String (Vhdsm.SR.slave_attach ctx tok sr_uuid host ids)
				| Sr_slave_detach (tok, sr_uuid, host) ->
					let () = Vhdsm.SR.slave_detach ctx tok sr_uuid host in Nil
				| Sr_slave_recover (tok, sr_uuid, master) ->
					let () = Vhdsm.SR.slave_recover ctx tok sr_uuid master in Nil
				| Sr_mode sr_uuid ->
					Sr_mode_response (Vhdsm.SR.mode ctx sr_uuid)
				| Vdi_slave_attach (host_uuid, sr_uuid, id, writable, is_reattach) ->
					Slave_attach_info (Vhdsm.VDI.slave_attach ctx host_uuid sr_uuid id writable is_reattach)
				| Vdi_get_slave_attach_info (sr_uuid, id) ->
					Slave_attach_info (Vhdsm.VDI.get_slave_attach_info ctx sr_uuid id)
				| Vdi_slave_activate (host_uuid, sr_uuid, id, is_reactivate) ->
					let () = Vhdsm.VDI.slave_activate ctx host_uuid sr_uuid id is_reactivate in	Nil
				| Vdi_slave_deactivate (host_uuid, sr_uuid, id) ->
					let () = Vhdsm.VDI.slave_deactivate ctx host_uuid sr_uuid id in	Nil
				| Vdi_slave_detach (host_uuid, sr_uuid, id) ->
					let () = Vhdsm.VDI.slave_detach ctx host_uuid sr_uuid id in Nil
				| Vdi_slave_reload (sr_uuid, ids) ->
					  let ids = 
						  if !Global.dummy  
						  then List.map (fun (id,sai) -> (id,{ sai with sa_leaf_path = Printf.sprintf "%s/%s" (Global.get_host_local_dummydir ()) sai.sa_leaf_path})) ids
						  else ids
					  in
					let () = Vhdsm.VDI.slave_reload ctx sr_uuid ids in Nil
				| Vdi_slave_leaf_coalesce_stop_and_copy (sr_uuid, id, leaf_path, new_leaf_path) ->
					  let leaf_path = 
						  if !Global.dummy
						  then Printf.sprintf "%s/%s" (Global.get_host_local_dummydir ()) leaf_path 
						  else leaf_path 
					  in
					  let new_leaf_path = 
						  if !Global.dummy
						  then Printf.sprintf "%s/%s" (Global.get_host_local_dummydir ()) new_leaf_path 
						  else new_leaf_path 
					  in
					  let () = Vhdsm.VDI.slave_leaf_coalesce_stop_and_copy
						  ctx sr_uuid id leaf_path new_leaf_path in Nil
(*					| Vdi_external_clone ->
					let sr_uuid = get_named_string "sr_uuid" args in
					let filename = get_named_string "filename" args in
					let result = to_vdi_info (Vhdsm.VDI.external_clone ctx sr_uuid filename) in
					XMLRPC.Success [result]*)
				| Vdi_slave_set_phys_size (sr_uuid, id, size) ->
					let () = Vhdsm.VDI.slave_set_phys_size ctx sr_uuid id size in Nil
				| Vdi_thin_provision_request_more_space (sr_uuid, host, sizes) ->
					Lvs (Vhdsm.VDI.thin_provision_request_more_space ctx sr_uuid host sizes)
				| Sr_thin_provision_check (sr_uuid) ->
					let () = Vhdsm.SR.thin_provision_check ctx sr_uuid in Nil
				| Host_set_dead host_uuid ->
					let () = Vhdsm.Host.set_dead ctx host_uuid in Nil
				| Host_rolling_upgrade_finished ->
					let () = Vhdsm.Host.rolling_upgrade_finished ctx in Nil
				| Debug_waiting_locks_get ->
					Waiting_locks (Nmutex.get_waiting_list ())
				| Debug_waiting_lock_unwait lock ->
					let () = Nmutex.unwait lock in Nil
				| Debug_vdi_get_leaf_path (sr_uuid, id) ->
					String (Vhdsm.Debug.vdi_get_leaf_path ctx sr_uuid id)
				| Debug_waiting_mode_set mode ->
					let () = Nmutex.set_waiting_mode mode in Nil
				| Debug_get_id_to_leaf_map sr_uuid ->
					  let metadata = Attachments.gmm sr_uuid in
					  let hashtbl = Mutex.execute metadata.Vhd_types.m_id_mapping_lock.Nmutex.m (fun () ->
						  Hashtbl.copy metadata.Vhd_types.m_data.Vhd_types.m_id_to_leaf_mapping) in
					  Id_to_leaf_map (Vhd_types.rpc_of_id_to_leaf_mapping_t hashtbl)
				| Debug_get_vhds sr_uuid ->
					  let metadata = Attachments.gmm sr_uuid in
					  let rpc = Vhd_records.get_vhd_records_rpc metadata.Vhd_types.m_data.Vhd_types.m_vhds in
					  Vhds rpc
				| Debug_get_attached_vdis sr_uuid ->
					  let metadata = Attachments.gsm sr_uuid in
					  let hashtbl = Mutex.execute metadata.Vhd_types.s_mutex.Nmutex.m (fun () ->
						  Hashtbl.copy metadata.Vhd_types.s_data.Vhd_types.s_attached_vdis) in
					  Attached_vdis (Vhd_types.rpc_of_attached_vdis_t hashtbl)
				| Debug_get_master_metadata sr_uuid ->
					  let metadata = Attachments.gmm sr_uuid in
					  Master_metadata (Vhd_types.rpc_of_master_sr_metadata_data metadata.Vhd_types.m_data)
				| Debug_get_slave_metadata sr_uuid ->
					  let metadata = Attachments.gsm sr_uuid in
					  Slave_metadata (Vhd_types.rpc_of_slave_sr_metadata_data metadata.Vhd_types.s_data)
				| Debug_get_vhd_container sr_uuid ->
					  let metadata = Attachments.gmm sr_uuid in
					  let container = metadata.Vhd_types.m_data.Vhd_types.m_vhd_container in
					  Vhd_container (Lvmabs_types.rpc_of_container_info container)
				| Debug_get_pid ->
					  Vhdd_pid (Unix.getpid ())
				| Debug_get_attach_finished sr_uuid ->
					  let metadata = Attachments.gmm sr_uuid in
					  Attach_finished metadata.Vhd_types.m_data.Vhd_types.m_attach_finished
				| Debug_slave_get_leaf_vhduid (sr_uuid,id) ->
					  let metadata = Attachments.gsm sr_uuid in
					  Mutex.execute metadata.Vhd_types.s_mutex.Nmutex.m (fun () ->
						  let v = Hashtbl.find metadata.Vhd_types.s_data.Vhd_types.s_attached_vdis id in
						  let leaf_path = v.Vhd_types.savi_attach_info.Int_types.sa_leaf_path in
						  let vhduid = Vhdutil.with_vhd leaf_path false (fun vhd -> 
							  Vhd.get_uid vhd) 
						  in
						  Vhduid vhduid)
				| Debug_write_junk (sr_uuid, id, size, n, current_junk) ->
					  debug "Writing junk";
					  let metadata = Attachments.gsm sr_uuid in
					  Mutex.execute metadata.Vhd_types.s_mutex.Nmutex.m (fun () -> 
						  let v = Hashtbl.find metadata.Vhd_types.s_data.Vhd_types.s_attached_vdis id in
						  let path = v.Vhd_types.savi_endpoint in
						  if !Global.dummy then Junk [] else Junk (Debug_utils.write_junk path size n current_junk))
				| Debug_check_junk (sr_uuid, id, junk) ->
					  debug "Checking junk";
					  let metadata = Attachments.gsm sr_uuid in
					  Mutex.execute metadata.Vhd_types.s_mutex.Nmutex.m (fun () -> 
						  let v = Hashtbl.find metadata.Vhd_types.s_data.Vhd_types.s_attached_vdis id in
						  let path = v.Vhd_types.savi_endpoint in
						  if !Global.dummy then () else Debug_utils.check_junk path junk);
					  debug "Junk OK (%d)" (List.length junk);
					  Nil
				| Debug_get_ready ->
					if !Global.ready then Nil else failwith "Not ready"
				| Debug_get_host ->
					  Host (Global.get_host_uuid ())
	  | Debug_die b ->
	      debug "Got instruction to die with restart=%b" b;
	      exit (if b then Global.restart_return_code else 0);
		  Nil)
		       
	with
		| IntError(string,args) ->
			log_backtrace ();
			debug "Caught error: %s, [%s]" string (String.concat "; " args);
			Failure(string,args)
		| e ->
			log_backtrace ();
			debug "Caught internal error: %s" (Printexc.to_string e);
			Failure ("INTERNAL_ERROR",[Printexc.to_string e])

let local_rpc task_id call =
	let context = Context.({c_driver="unknown"; c_api_call=""; c_task_id=task_id; c_other_info=[]}) in
	debug "Got internal call: %s" (Jsonrpc.to_string call);
	Int_rpc.rpc_of_intrpc_response_wrapper (process context (Int_rpc.intrpc_of_rpc call))

