(* Slave! *)
open Vhd_types
open Threadext
open Xstringext
open Drivers
open Int_types
open Listext

module D = Debug.Make(struct let name="vhdSlave" end)
open D

let get_slave_attach_dir sr_uuid =
	Printf.sprintf "%s/var/run/vhdd/slave_attach/%s" (Global.get_host_local_dummydir ()) sr_uuid

let fix_ctx context vdi =
	Tracelog.add_to_c_other_info context "module" "vhdSlave";
	match vdi with 
		| Some id-> Tracelog.add_to_c_other_info context "vdi" id 
		| None -> ()

let string_of_slave_operation op = match op with
  | Attaching -> "Attaching"
  | Reattaching -> "Reattaching"
  | Activating -> "Activating"
  | Deactivating -> "Deactivating"
  | Detaching -> "Detaching"
  | AttachAndActivating -> "AttachAndActivating"
  | LeafCoalescing -> "LeafCoalescing"
  | Reactivating -> "Reactivating"

module VDI = struct

	let commit_slave_attach_info_to_disk sr_uuid id slave_attach_info =
		let slave_attach_dir = get_slave_attach_dir sr_uuid in
		Unixext.mkdir_rec slave_attach_dir 0o777;
		let slave_attach_file = Printf.sprintf "%s/%s" slave_attach_dir id in
		Unixext.write_string_to_file slave_attach_file (Jsonrpc.to_string (rpc_of_slave_attach_info slave_attach_info))

	let remove_slave_attach_info_from_disk sr_uuid id =
		let slave_attach_dir = get_slave_attach_dir sr_uuid in
		Unixext.mkdir_rec slave_attach_dir 0o777;
		let slave_attach_file = Printf.sprintf "%s/%s" slave_attach_dir id in
		Unixext.unlink_safe slave_attach_file

	let with_op_inner context msg1 tl1 msg2 tl2 hashtbl metadata id op f =
		debug "s_mutex lock: with_op_inner";
		Nmutex.execute context metadata.s_mutex msg1 (fun () ->
			if not metadata.s_data.s_ready && (not (op=Reattaching || op=AttachAndActivating)) then begin
			  debug "Failing operation %s: not ready" (string_of_slave_operation op);
			  failwith "Not ready"
			end;
			debug "Checking current ops";
			while Hashtbl.mem hashtbl id do
				debug "Waiting for a current op to finish";
				Nmutex.condition_wait context metadata.s_condition metadata.s_mutex
			done;
			debug "Adding my current op to the current_ops hashtbl";
			Hashtbl.add hashtbl id op;
			Tracelog.append context tl1 None;
			Html.signal_slave_metadata_change metadata ();
		);
		Pervasiveext.finally f (fun () ->
			debug "s_mutex lock: with_op_inner finally clause";
			Nmutex.execute context metadata.s_mutex msg2 (fun () ->
				debug "Removing my current op";
				Tracelog.append context tl2 None;
				Hashtbl.remove hashtbl id;
				Html.signal_slave_metadata_change metadata ();
				debug "Broadcasting to wake up other threads";
				Nmutex.condition_broadcast context metadata.s_condition
			))

	let with_op context metadata id op = with_op_inner context "Adding to current_operations" (Tracelog.Slave_s_current_ops_add (id,op)) "Removing from current_operations" (Tracelog.Slave_s_current_ops_remove id) metadata.s_data.s_current_ops metadata id op

	let with_master_approved_op context metadata id op = with_op_inner context "Adding to master_approved_ops" (Tracelog.Slave_s_master_approved_ops_add (id,op)) "Removing from master_approved_ops" (Tracelog.Slave_s_master_approved_ops_remove id) metadata.s_data.s_master_approved_ops metadata id op

	let attach_from_sai context metadata id slave_attach_info =
	  let sr_uuid = metadata.s_data.s_sr in
          
	  let cleanup_funcs = ref [] in
          let add_to_cleanup_funcs f =
            cleanup_funcs := f :: (!cleanup_funcs) in
          let exec_cleanup_funcs () =
            List.iter (fun f -> f ()) !cleanup_funcs
          in
	  
	  List.iter (fun dmn ->
	    debug "LV name: %s" (match dmn with | Mlvm x -> x.dmn_dm_name)) slave_attach_info.sa_lvs;
	  
	  try
			(* Attach the LVs if necessary *)
	    begin
	      try
		List.iter (fun dmn ->
		  ignore(Host.attach_lv dmn);
		  add_to_cleanup_funcs (fun () -> Host.remove_lv dmn)
		) slave_attach_info.sa_lvs
	      with e ->
		log_backtrace ();
		debug "Caught exception: %s" (Printexc.to_string e);
		failwith "Could not attach all LVs"
	    end;
		
	    let (tapdev,endpoint) = Tapdisk.attach sr_uuid id in
	    add_to_cleanup_funcs (fun () -> Tapdisk.detach tapdev sr_uuid id);
	    
	    debug "s_mutex lock: attach_from_sai";
	    Nmutex.execute context metadata.s_mutex "Adding info to s_attached_vdis"
	      (fun () ->
		let savi= {
		  savi_attach_info = slave_attach_info;
		  savi_blktap2_dev = tapdev;
		  savi_resync_required = false;
		  savi_endpoint = endpoint;
		  savi_link = Tapdisk.get_vhd_link sr_uuid id slave_attach_info.sa_leaf_path;
		  savi_maxsize = slave_attach_info.sa_leaf_maxsize;
		  savi_phys_size = slave_attach_info.sa_leaf_phys_size;
		  savi_activated = false;
		  savi_paused=false;
		} in
		Hashtbl.replace metadata.s_data.s_attached_vdis id savi;
		Tracelog.append context (Tracelog.Slave_s_attached_vdis_add (id,savi)) None;	
		Html.signal_slave_metadata_change metadata ();
	      );
	    endpoint
	  with e ->
	    error "Caught unexpected exception: %s" (Printexc.to_string e);
	    exec_cleanup_funcs ();
	    raise e
	      
	let attach context metadata vdi writable =
	  let id = vdi in
	  fix_ctx context (Some id);

	  if metadata.s_data.s_master = None then 
	    failwith "Can't attach a VDI without a master set";

	  let host_uuid = Global.get_host_uuid () in
	  let sr_uuid = metadata.s_data.s_sr in

	  let params = with_op context metadata id Attaching (fun () ->
	    let current = Nmutex.execute context metadata.s_mutex "Finding whether we're already attached"
	      (fun () ->
		try
		  let current = Hashtbl.find metadata.s_data.s_attached_vdis id in
		  Some current
		with _ ->
		  None)
	    in

	    match current with
	    | None ->
	      let slave_attach_info = 
		let x = Int_client_utils.slave_retry_loop context [e_unknown_location] (fun client -> 
		  let module Client = (val client : Int_client.CLIENT) in
		  Client.VDI.slave_attach ~host_uuid ~sr:sr_uuid ~vdi:id ~writable ~is_reattach:false) metadata in
		if !Global.dummy 
		then 
		  { x with 
		    sa_leaf_path = 
		      Printf.sprintf "%s/%s" (Global.get_host_local_dummydir ()) x.sa_leaf_path
		  }
		else x
	      in
	      debug "Got response: leaf=%s" slave_attach_info.sa_leaf_path;
	      
	      with_master_approved_op context metadata id Attaching (fun () -> 
		try 
		  commit_slave_attach_info_to_disk sr_uuid id slave_attach_info;
		  attach_from_sai context metadata id slave_attach_info
		with e ->
		  log_backtrace ();
		  error "Caught exception while attaching: %s" (Printexc.to_string e);
		  remove_slave_attach_info_from_disk sr_uuid id;
		  Int_client_utils.slave_retry_loop context [] (fun client -> 
		    let module Client = (val client : Int_client.CLIENT) in
		    Client.VDI.slave_detach ~host_uuid ~sr:sr_uuid ~vdi:id) metadata;
		  error "Issued slave_detach";
		  raise e)
	    | Some current ->
	      current.savi_endpoint) in
	  Storage_interface.({params; xenstore_data=[]})

	let unpause sr_uuid id savi =
		if savi.savi_activated then begin
			ignore(Tapdisk.make_vhd_link sr_uuid id savi.savi_attach_info.sa_leaf_path);
			Tapdisk.t_unpause savi.savi_blktap2_dev savi.savi_link
				(if savi.savi_attach_info.sa_leaf_is_raw then Tapctl.Aio else Tapctl.Vhd);
		end

	let reattach_from_sai context metadata vdi sai =
		let id = vdi in
		fix_ctx context (Some id);

		(* If we've got slave_attach_info, the master knows we're reattaching *)
		with_master_approved_op context metadata id Reattaching (fun () ->

			(* Make sure that we're attached... *)
			Nmutex.execute context metadata.s_mutex "Making sure we're attached" (fun () ->
				if not (Hashtbl.mem metadata.s_data.s_attached_vdis id) then
					raise (Int_rpc.IntError (e_not_attached, [id])));
			
			commit_slave_attach_info_to_disk metadata.s_data.s_sr id sai;

			Nmutex.execute context metadata.s_mutex "Performing inner part of reattach" (fun () ->
				let old_savi = Hashtbl.find metadata.s_data.s_attached_vdis id in
				let old_sai = old_savi.savi_attach_info in
				let savi = 
					if sai <> old_sai 
					then begin
						let get_name = function | Mlvm dm -> dm.dmn_dm_name in
						let find_lv name lvs = List.find (function | Mlvm dm -> dm.dmn_dm_name=name ) lvs in
						
						let old_lvs = List.map get_name old_sai.sa_lvs in
						let new_lvs = List.map get_name sai.sa_lvs in
						
						let (unwanted,common,newer) =
							(List.set_difference old_lvs new_lvs,
							List.intersect old_lvs new_lvs,
							List.set_difference new_lvs old_lvs) in
						
						List.iter (fun dm_name ->
							let lv = find_lv dm_name sai.sa_lvs in
							ignore(Host.attach_lv lv)
						) newer;
						
						if old_savi.savi_activated then begin
							debug "Pausing tapdisk...";
							Tapdisk.t_pause old_savi.savi_blktap2_dev
						end;
						
						List.iter (fun dm_name ->
							let dmn = find_lv dm_name old_sai.sa_lvs in
							Host.remove_lv dmn
						) unwanted;
						
						List.iter (fun dm_name ->
							let old_dmn = find_lv dm_name old_sai.sa_lvs in
							let new_dmn = find_lv dm_name sai.sa_lvs in
							match old_dmn with
								| Mlvm dm ->
									  if old_dmn <> new_dmn then
										  Host.change_lv new_dmn
						) common;
						
						
						let new_savi = { old_savi with
							savi_attach_info = sai;
							savi_phys_size=0L;
							savi_maxsize=sai.sa_leaf_maxsize;
							savi_resync_required=false;
							savi_paused=false; }
						in
						new_savi
					end 
					else old_savi 
				in
				
				if sai<>old_sai then 
					unpause metadata.s_data.s_sr id savi;

				Hashtbl.replace metadata.s_data.s_attached_vdis id savi;
				Tracelog.append context (Tracelog.Slave_s_attached_vdis_update (id,savi)) None;	

				Html.signal_slave_metadata_change metadata ()))

		let reactivate context metadata vdi =
			let id = vdi in

			with_op context metadata id Reactivating (fun () ->
				Nmutex.execute context metadata.s_mutex "Reactivating if necessary" (fun () -> 
					let new_savi = Hashtbl.find metadata.s_data.s_attached_vdis id in
					if new_savi.savi_activated then begin
						Int_client_utils.slave_retry_loop context [] (fun client -> 
						  let module Client = (val client : Int_client.CLIENT) in
						  
						  Client.VDI.slave_activate ~host_uuid:(Global.get_host_uuid ()) ~sr:metadata.s_data.s_sr ~vdi:id ~is_reactivate:true) metadata
					end;
				))


	let reattach context metadata vdi =
		let id = vdi in
		fix_ctx context (Some id);
		debug "Reattaching vdi: %s" id;
		
		if metadata.s_data.s_master = None then
			failwith "Can't reattach without a master";
		
		with_op context metadata id Reattaching (fun () -> 
			(* Make sure that we're attached... *)
			let writable = Nmutex.execute context metadata.s_mutex "Making sure we're attached" (fun () ->
				try 
					let savi = Hashtbl.find metadata.s_data.s_attached_vdis id in
					savi.savi_attach_info.sa_writable
				with Not_found -> 
					raise (Int_rpc.IntError (e_not_attached, [id])))
			in

			(* We can't be detached because of the 'with_op' fn above, so now we're safe *)
			let sai = 
				Int_client_utils.slave_retry_loop context [] (fun client -> 
				  let module Client = (val client : Int_client.CLIENT) in
				  let x = Client.VDI.slave_attach ~host_uuid:(Global.get_host_uuid ()) ~sr:metadata.s_data.s_sr ~vdi:id ~writable ~is_reattach:true in
				  if !Global.dummy 
				  then 
				    { x with 
				      sa_leaf_path = 
					Printf.sprintf "%s/%s" (Global.get_host_local_dummydir ()) x.sa_leaf_path
				    }
				  else x
				) metadata in
			
			debug "Got response: leaf=%s" sai.sa_leaf_path;
			
			reattach_from_sai context metadata vdi sai)

	let detach context metadata vdi =
		let id = vdi in
		fix_ctx context (Some id);
		(* Nb. tapdisk _should_ be already dead at this point *)


		if metadata.s_data.s_master=None then
			failwith "Can't detach without a master";
			
		with_op context metadata id Detaching (fun () ->

			let current = Nmutex.execute context metadata.s_mutex "Checking we're attached" (fun () ->
				try Some (Hashtbl.find metadata.s_data.s_attached_vdis id) with Not_found -> None)
			in

			match current with
				| Some savi ->
					(* If there's a blktap device associated with this, kill
					   it *)
					let x = savi.savi_blktap2_dev in
					Nmutex.execute context metadata.s_mutex "Deactivating tapdisk if necessary" (fun () -> 
						if Tapdisk.get_activated x then begin
							debug "WARNING: Tapdisk not deactivated at detach time";
							Tapdisk.deactivate x metadata.s_data.s_sr id savi.savi_attach_info.sa_leaf_path;
							savi.savi_activated <- false;
							Html.signal_slave_metadata_change metadata ();
						end);

					Tapdisk.detach x metadata.s_data.s_sr id;

					begin
						try
							Int_client_utils.slave_retry_loop context [e_not_attached] (fun client -> 
							  let module Client = (val client : Int_client.CLIENT) in

							  Client.VDI.slave_detach ~host_uuid:(Global.get_host_uuid ()) 
							    ~sr:metadata.s_data.s_sr ~vdi:id) metadata
						with Int_rpc.IntError(e,args) as exn ->
							if e=e_not_attached
							then debug "Ignoring not_attached exception from master"
							else raise exn
					end;
					with_master_approved_op context metadata id Detaching (fun () ->
						(* If there's any LVM volumes, remove them *)

						let current = Nmutex.execute context metadata.s_mutex "Reloading attach info" (fun () ->
							try Some (Hashtbl.find metadata.s_data.s_attached_vdis id) with Not_found -> None)
						in

						match current with
							| Some savi ->
								let sa = savi.savi_attach_info in
								List.iter (fun dmn ->
									try
										Host.remove_lv dmn
									with e ->
										debug "Ignoring exception while trying to remove LV: %s" (Printexc.to_string e))
									sa.sa_lvs;
						
								(* Remove this VDI from the attached VDIs hashtbl *)
								Nmutex.execute context metadata.s_mutex "Removing attach info" (fun () ->
									Hashtbl.remove metadata.s_data.s_attached_vdis id);
								Tracelog.append context (Tracelog.Slave_s_attached_vdis_remove id) None;
								Html.signal_slave_metadata_change metadata ();

								(* Remove the attachment info from disk *)
								remove_slave_attach_info_from_disk metadata.s_data.s_sr id
							| None ->
								failwith "Well that's pretty odd. What's going on here?")
				| None ->
					(* If it's not attached, the detach shouldn't fail! *)
					())

	let activate_unsafe context metadata id =
		fix_ctx context (Some id);
		Nmutex.execute context metadata.s_mutex "Activating tapdisk" (fun () ->
			let savi = Hashtbl.find metadata.s_data.s_attached_vdis id in
			Tapdisk.activate savi.savi_blktap2_dev metadata.s_data.s_sr id savi.savi_attach_info.sa_leaf_path
			  (if savi.savi_attach_info.sa_leaf_is_raw then Tapctl.Aio else Tapctl.Vhd);
			debug "Setting maxsize in shared page";
			(match savi.savi_attach_info.sa_leaf_maxsize with
			| Some s ->
			  debug "Setting maxsize=%Ld" s;
			  Tapdisk_listen.write_maxsize (metadata.s_data.s_sr,id) s);
			savi.savi_activated <- true;
			Html.signal_slave_metadata_change metadata ();
		)

	let activate context metadata vdi =
		let id = vdi in
		fix_ctx context (Some id);

		if metadata.s_data.s_master = None then
			failwith "Can't activate without a master";

		with_op context metadata id Activating (fun () ->
			if not (* activated *) (Nmutex.execute context metadata.s_mutex "Checking whether the VDI is activated"
				(fun () ->
					if not (Hashtbl.mem metadata.s_data.s_attached_vdis id) then failwith "Not attached: cannot activate";
					let savi = Hashtbl.find metadata.s_data.s_attached_vdis id in
					savi.savi_activated))
			then begin
				Int_client_utils.slave_retry_loop context [e_vdi_active_elsewhere] (fun client -> 
				  let module Client = (val client : Int_client.CLIENT) in

				  Client.VDI.slave_activate 
				    ~host_uuid:(Global.get_host_uuid ()) ~sr:metadata.s_data.s_sr ~vdi:id ~is_reactivate:false) metadata;
				with_master_approved_op context metadata id Activating (fun () -> 
					try
						activate_unsafe context metadata id
					with e ->
						log_backtrace ();
						debug "Caught exception while activating: %s" (Printexc.to_string e);
						(* Failed - deactivate on master *)
						Int_client_utils.slave_retry_loop context [] (fun client -> 
						  let module Client = (val client : Int_client.CLIENT) in
						  
						  Client.VDI.slave_deactivate ~host_uuid:(Global.get_host_uuid ()) 
						    ~sr:metadata.s_data.s_sr ~vdi:id) metadata;
						raise e)
			end)

	let deactivate context metadata vdi =
		let id = vdi in
		fix_ctx context (Some id);

		if metadata.s_data.s_master = None then
			failwith "Can't deactivate without a master";

		with_op context metadata id Deactivating (fun () ->
			Nmutex.execute context metadata.s_mutex "Deactivating"
				(fun () ->
					if not (Hashtbl.mem metadata.s_data.s_attached_vdis id) then failwith "Not attached: cannot deactivate";
					let savi = Hashtbl.find metadata.s_data.s_attached_vdis id in
					if not savi.savi_activated then failwith "Not activated: cannot deactivate";

					(* Kill the tapdisk *)
					begin
						let rec deactivate_loop n =
							if n=0 then failwith "Failed to deactivate!";
							try
								Tapdisk.deactivate savi.savi_blktap2_dev metadata.s_data.s_sr id savi.savi_attach_info.sa_leaf_path;
							with _ ->
								Thread.delay 1.0; (* WARNING WARNING - sleeping with lock held!!! (shouldn't happen though) *)
								deactivate_loop (n-1)
						in
						savi.savi_activated <- false;
						Html.signal_slave_metadata_change metadata ();
						deactivate_loop 30
					end;
					savi.savi_activated <- false);
			try
				Int_client_utils.slave_retry_loop context [e_not_activated] (fun client -> 
				  let module Client = (val client : Int_client.CLIENT) in

				  Client.VDI.slave_deactivate ~host_uuid:(Global.get_host_uuid ()) ~sr:metadata.s_data.s_sr ~vdi:id) metadata
			with
				| Int_rpc.IntError(e,args) as exn ->
					if (e=e_not_activated)
					then debug "Ignoring not_activated error"
					else raise exn
		)


	let slave_reload context metadata ids =
		fix_ctx context None;
		debug "Got slave_reload call on the following VDIs:";
		List.iter (fun (id,sai) ->
			debug "id: %s" id;
			reattach_from_sai context metadata id sai) ids


	(* If there are network problems during this call, the master will reissue the
	   same call. Therefore this needs to be idempotent. Hence the test to see if
	   new_leaf_path is equal to sa_leaf_path. *)
	let slave_leaf_coalesce_stop_and_copy context metadata id leaf_path new_leaf_path =
		fix_ctx context (Some id);
		with_master_approved_op context metadata id LeafCoalescing (fun () ->
			let savi = Hashtbl.find metadata.s_data.s_attached_vdis id in
			debug "leaf_path: %s new_leaf_path: %s sa_leaf_path: %s" leaf_path new_leaf_path savi.savi_attach_info.sa_leaf_path;
			if leaf_path = savi.savi_attach_info.sa_leaf_path then begin
				Nmutex.execute context metadata.s_mutex "leaf coalesce stop and copy (with mutex held!?)" (fun () ->
					if savi.savi_activated then Tapdisk.t_pause savi.savi_blktap2_dev;
					Leaf_coalesce.slave_leaf_coalesce_copy context metadata savi.savi_attach_info.sa_leaf_path;
					if savi.savi_activated then
						Tapdisk.activate savi.savi_blktap2_dev metadata.s_data.s_sr id new_leaf_path
							(if savi.savi_attach_info.sa_leaf_is_raw then Tapctl.Aio else Tapctl.Vhd))
			end else
				if new_leaf_path = savi.savi_attach_info.sa_leaf_path
				then ()
				else begin
					debug "new_leaf_path: %s" new_leaf_path;
					debug "sa_leaf_path: %s" savi.savi_attach_info.sa_leaf_path;
					failwith "Leaf info incorrect!"
				end)

	let slave_set_phys_size context  metadata id size =
		fix_ctx context (Some id);
		let check = Nmutex.execute context metadata.s_mutex "Setting the phys_size" (fun () ->
			let savi=Hashtbl.find metadata.s_data.s_attached_vdis id in
			match savi.savi_maxsize with
				| Some max -> begin
					let newsavi={savi with savi_phys_size = size} in
					Hashtbl.replace metadata.s_data.s_attached_vdis id newsavi;
					Tracelog.append context (Tracelog.Slave_s_attached_vdis_update (id,newsavi)) None;
					Html.signal_slave_metadata_change metadata ();
					let check = Int64.sub max size in
					if check < Vhdutil.tp_emergency_threshold
					then begin
						Tapdisk.t_pause savi.savi_blktap2_dev;
						debug "Paused tapdisk id=%s" id;
						savi.savi_paused <- true;
						Html.signal_slave_metadata_change metadata ();
					end;
					check
				end
				| None -> Vhdutil.tp_overhead)
		in

		Mutex.execute Global.min_size_lock (fun () -> if check < !Global.min_size then Global.min_size := check);
		debug "slave_set_phys_size: check=%Ld thresh=%Ld" check Vhdutil.tp_threshold;

		if check < Vhdutil.tp_threshold
		then
			ignore(Thread.create (fun () ->
			  debug "thin_provision_check call thread created";
			  Int_client.LocalClient.SR.thin_provision_check ~sr:metadata.s_data.s_sr) ())
		else
		  debug "not bothering"


	let generate_config context metadata device_config vdi =
		let id = vdi in
		fix_ctx context (Some id);
		let sr_uuid = metadata.s_data.s_sr in
		let slave_attach_info = 
		  Int_client_utils.slave_retry_loop context [] (fun client -> 
		    let module Client = (val client : Int_client.CLIENT) in
		    
		    let x = Client.VDI.get_slave_attach_info ~sr:sr_uuid ~vdi:id in
		    if !Global.dummy 
		    then 
		      { x with 
			sa_leaf_path = 
			  Printf.sprintf "%s/%s" (Global.get_host_local_dummydir ()) x.sa_leaf_path
		      }
		    else x) metadata 
		in
		let arg = Jsonrpc.to_string (rpc_of_slave_attach_info slave_attach_info) in
(*		let call = Smapi_client.make_call ~vdi_location:id device_config (Some metadata.s_data.s_sr) "vdi_attach_from_config" [ arg ] in
		let str = Xml.to_string (Smapi_client.xmlrpc_of_call call) in
		str*) ""

	let attach_and_activate_from_config context metadata device_config vdi slave_attach_info =
		let id = vdi in
		fix_ctx context (Some id);

		with_op context metadata id AttachAndActivating (fun () ->
			let current = Nmutex.execute context metadata.s_mutex "Getting current attach info"
				(fun () ->
					try
						let current = Hashtbl.find metadata.s_data.s_attached_vdis id in
						Some current
					with _ ->
						None)
			in			
			
			match current with
				| None ->
					let endpoint = attach_from_sai context metadata id slave_attach_info in
					activate_unsafe context metadata id;
					Storage_interface.({params=endpoint; xenstore_data=[]})

				| Some savi -> 
					if not savi.savi_activated 
					then activate_unsafe context metadata id; 
					Storage_interface.({params=savi.savi_endpoint; xenstore_data=[]}))
			
end

module SR = struct

	(* This function copes with two situations: Attach fresh, and reattach.

	   The first thing it does, therefore, is to scan two things: The status of the tapdisks,
	   and the files in the attach directory.

	   It is the intersection of active tapdisks and files that
	   determine which disks we are actually going to attempt to keep
	   active, and which we are going to detach. So if we have a file in
	   the attach dir without an active tapdisk, we will call to the
	   master to detach that disk. If we have an active tapdisk without
	   a file in the attach dir, we'll kill the tapdisk.

	   Note that while we were away, it's possible that the master did some stuff
	   to disks that we're using. We don't try to resynchronise, but leave
	   it up to the task (which will still be in progress trying to talk to
	   us) to tell us if we need to change anything.
	*)

	let attach context device_config path sr =
		fix_ctx context None;
		debug "VhdSlave.SR.attach";

		(* We do a scan here to check if this is a 'reattach after dying' *)
		let tapdisk_srs_and_ids = Tapdisk.scan () in
		let attached_vdis = Hashtbl.create 10 in

		let scan_attachments () =
			let slave_attach_dir = get_slave_attach_dir sr in
			Unixext.mkdir_rec slave_attach_dir 0o700;
			let rec inner acc dh =
				try
					inner ((Unix.readdir dh)::acc) dh
				with End_of_file ->
					acc
			in

			(* Scan the slave_attachment files *)
			let files = List.filter (fun f -> f<>"." && f<>"..") (Unixext.with_directory slave_attach_dir (inner [])) in
			List.iter (fun slave_attach_file ->
				let id = slave_attach_file in
				debug "Checking slave_attach_file: %s" id;
				let json = Jsonrpc.of_string (Unixext.string_of_file (Printf.sprintf "%s/%s" slave_attach_dir slave_attach_file)) in
				let slave_attach_info = slave_attach_info_of_rpc json in

				let tapdev_and_link_opt =
					try
						let (tapdev,_,_,link) =
							List.find (fun (_,sr',id',_) -> sr' = sr && id' = id) tapdisk_srs_and_ids
						in
						Some (tapdev,link)
					with Not_found ->
						None
				in

				match tapdev_and_link_opt with
					| Some (tapdev,link) ->
						let paused = Tapdisk.get_paused tapdev in
						let activated = link <> None in

						let new_savi = { savi_attach_info = slave_attach_info;
							savi_blktap2_dev = tapdev;
							savi_resync_required = true;
							savi_endpoint = Tapdisk.get_tapdev_link sr id;
							savi_link = Tapdisk.get_vhd_link sr id slave_attach_info.sa_leaf_path;
							savi_phys_size = 0L;
							savi_maxsize = slave_attach_info.sa_leaf_maxsize;
							savi_activated = activated;
							savi_paused = paused;
							} in
						Hashtbl.replace attached_vdis id new_savi;

						(* Pausing is only ever for in-progress operations. If the tapdisk is
						   paused therefore, it needs to be unpaused! *)

						if activated then begin
							if not paused then
								Tapdisk.t_pause tapdev;
							VDI.unpause sr id new_savi
						end;

						(* It's possible that we leak DM nodes *)
						List.iter (fun dmn -> Host.bump_refcount dmn) slave_attach_info.sa_lvs
					| None ->
						  debug "Couldn't find the tapdev and link. Removing attach info";
						VDI.remove_slave_attach_info_from_disk sr id
			) files
		in scan_attachments ();

		let s_thin_provisioning = List.mem_assoc "reservation_mode" device_config &&
		  match List.assoc "reservation_mode" device_config with
		  | "thin" -> true
		  | _ -> false
		in

		let s_current_ops = Hashtbl.create 10 in
		let s_master_approved_ops = Hashtbl.create 10 in
		Tracelog.append context (Tracelog.Slave_s_master None) None;
		Tracelog.append context (Tracelog.Slave_s_current_ops s_current_ops) None;
		Tracelog.append context (Tracelog.Slave_s_master_approved_ops s_master_approved_ops) None;
		Tracelog.append context (Tracelog.Slave_s_attached_vdis (Hashtbl.copy attached_vdis)) None;
		Tracelog.append context (Tracelog.Slave_s_ready false) None;
		Tracelog.append context (Tracelog.Slave_s_thin_provision_request_in_progress false) None;
		{
			s_mutex=Nmutex.create "s_mutex";
			s_condition=Nmutex.create_condition "s_condition";
			s_idx=0;
			s_data = {
				s_path=path;
				s_master=None;
				s_ready=false;
				s_current_ops=Hashtbl.create 10;
				s_master_approved_ops=Hashtbl.create 10;
				s_attached_vdis=attached_vdis;
				s_sr=sr;
				s_thin_provisioning;
				s_thin_provision_request_in_progress=false
			}
		}

	let register_with_master context metadata master =
		fix_ctx context None;
		debug "Registering with master";

		metadata.s_data.s_master <- Some master;
		Tracelog.append context (Tracelog.Slave_s_master (Some master)) (Some "About to register with the master");

		Html.signal_slave_metadata_change metadata ();
						
		let attached_vdis_list =
			Nmutex.execute context metadata.s_mutex "Finding all attached/activated VDIs" (fun () ->
				Hashtbl.fold (fun k v acc -> (k,v.savi_activated)::acc) metadata.s_data.s_attached_vdis [])
		in

		Int_client_utils.slave_retry_loop context []
			(fun client ->
			  let module Client = (val client : Int_client.CLIENT) in
			  ignore(Client.SR.slave_attach ~sr:metadata.s_data.s_sr ~host:(Global.get_localhost ())
					~vdis:attached_vdis_list)) metadata;

		debug "Registration functions finished. Setting s_ready=true";

		metadata.s_data.s_ready <- true;
		Tracelog.append context (Tracelog.Slave_s_ready true) None;
		Html.signal_slave_metadata_change metadata ()

	let assert_can_detach context metadata device_config =
		assert(Hashtbl.length metadata.s_data.s_attached_vdis = 0)

	let detach context metadata device_config =
		()

	let slave_recover context metadata tok master =
	  debug "Recover called";
            
		let currently_attached =
			Nmutex.execute context metadata.s_mutex "Finding all currently attached VDIs" (fun () ->
				Hashtbl.fold (fun k v acc -> (k,v)::acc) metadata.s_data.s_attached_vdis [])
		in
		List.iter (fun (k,v) ->
			try
				let vdi = k in
				VDI.reattach context metadata vdi;
				VDI.reactivate context metadata vdi
			with e ->
				log_backtrace ();
				warn "Error: caught exception while reattaching VDI: Detaching! (%s)" (Printexc.to_string e);
				VDI.detach context metadata k
		) currently_attached;
		let has_master_changed = 
			match metadata.s_data.s_master with 
				| Some m2 -> m2 <> master 
				| None -> true
		in
		if has_master_changed then begin
			metadata.s_data.s_master <- Some master;
			Tracelog.append context (Tracelog.Slave_s_master (Some master)) (Some "Master has changed!");
			Html.signal_slave_metadata_change metadata ();
			Attachments.log_attachment_new_master metadata.s_data.s_sr (Some master)
		end

	let thin_provision_check context metadata =
	  fix_ctx context None;
	  debug "Thin provision check";
	  let in_progress =
	    Nmutex.execute context metadata.s_mutex "Checking whether thin provision request is in progress" (fun () ->
	      let cur = metadata.s_data.s_thin_provision_request_in_progress in
	      metadata.s_data.s_thin_provision_request_in_progress <- true;
	      Tracelog.append context (Tracelog.Slave_s_thin_provision_request_in_progress true) None;
	      Html.signal_slave_metadata_change metadata ();
	      cur)
	  in
	  if in_progress then begin
	    debug "thin provision request is already in progress. returning"
	  end else begin
	    try
	      debug "checkpoint 1";
	      if metadata.s_data.s_thin_provisioning then
		let rec inner () =
		  debug "checkpoint 2";
		  let need_resizing = Nmutex.execute context metadata.s_mutex "Checking who needs resizing" (fun () ->
		    Hashtbl.fold (fun k v acc ->
		      match v.savi_maxsize with
		      | None -> acc
		      | Some max ->
			let check = Int64.sub max v.savi_phys_size in
			debug "check=%Ld" check;
			if check < Vhdutil.tp_lower_threshold
			then ({vs_id=k; vs_size=v.savi_phys_size}::acc)
			else acc) metadata.s_data.s_attached_vdis []) in
		  if List.length need_resizing > 0 then begin
		    let new_attach_infos = 
		      Int_client_utils.slave_retry_loop context [] (fun client -> 
			let module Client = (val client : Int_client.CLIENT) in
			
			Client.VDI.thin_provision_request_more_space ~sr:metadata.s_data.s_sr ~host_uuid:(Global.get_host_uuid ()) ~sizes:need_resizing) metadata in
		    debug "New attach infos: length=%d" (List.length new_attach_infos);
		    List.iter (function ai ->
		      Nmutex.execute context metadata.s_mutex "Changing VDI" (fun () ->
			debug "Changing LV: %s" ai.dmn_dm_name;
			match Hashtbl.fold (fun vdi old_savi acc ->
			  if Filename.basename (old_savi.savi_attach_info.sa_leaf_path) = ai.dmn_dm_name
			  then begin
			    Host.change_lv (Mlvm ai);
			    let sectors = List.fold_left (fun acc x -> Int64.add acc x.Camldm.len) 0L (Array.to_list ai.dmn_mapping.Camldm.m) in
			    let maxsize = Int64.mul sectors 512L in
			    Tapdisk_listen.write_maxsize (metadata.s_data.s_sr,vdi) maxsize;
			    (if old_savi.savi_paused then
				(debug "Resuming tapdisk: id=%s" vdi;
				 Tapdisk.activate old_savi.savi_blktap2_dev metadata.s_data.s_sr vdi old_savi.savi_attach_info.sa_leaf_path
				   (if old_savi.savi_attach_info.sa_leaf_is_raw then Tapctl.Aio else Tapctl.Vhd)));
			    Some (vdi,{
			      old_savi with 
				savi_maxsize = Some (Int64.mul 512L
						       (Array.fold_left (fun size map -> Int64.add map.Camldm.len size) 0L ai.dmn_mapping.Camldm.m));
				savi_attach_info = { old_savi.savi_attach_info with
				  sa_lvs = List.map (fun x ->
				    match x with | Mlvm d -> if d.dmn_dm_name = ai.dmn_dm_name then Mlvm ai else x) old_savi.savi_attach_info.sa_lvs } } )
			      
			  end else acc) metadata.s_data.s_attached_vdis None
			with
			| None -> ()
			| Some (k,v) -> 
			  (Hashtbl.replace metadata.s_data.s_attached_vdis k v; 
			   Tracelog.append context (Tracelog.Slave_s_attached_vdis_update (k,v)) None;
			   
			   Html.signal_slave_metadata_change metadata ()))) new_attach_infos;
		    
		    inner ()
		  end
		in inner ();
	      else
		debug "thin provisioning not enabled";
	      
	      Nmutex.execute context metadata.s_mutex "Unsetting thin provision request in progress" (fun () ->
		Tracelog.append context (Tracelog.Slave_s_thin_provision_request_in_progress false) None;
		metadata.s_data.s_thin_provision_request_in_progress <- false;
		Html.signal_slave_metadata_change metadata ();
	      )	
	    with e ->
	      log_backtrace ();
	      debug "Caught exception while resizing: %s" (Printexc.to_string e);
	      Nmutex.execute context metadata.s_mutex "Unsetting thin provision request in progress" (fun () ->
		metadata.s_data.s_thin_provision_request_in_progress <- false;
		Html.signal_slave_metadata_change metadata ();
	      )
	  end

end
