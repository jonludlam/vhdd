(* The master! *)
open Int_types
open Vhd_types
open Stringext
open Drivers
open Threadext
open Listext
open Coalesce
open Vhd_records
open Storage_interface

module D=Debug.Make(struct let name="vhdmaster" end)
open D

exception Other_master_detected of host

let fix_ctx context vdi =
	Tracelog.add_to_c_other_info context "module" "vhdMaster";
	match vdi with 
		| Some id-> Tracelog.add_to_c_other_info context "vdi" id 
		| None -> ()

module VDI = struct
	let check_all_hosts_present context metadata =
		let hosts = Slave_sr_attachments.get_attached_hosts context metadata in
		if List.exists (fun host -> host.ssa_resync_required) hosts then
			failwith "Not all hosts synced"

	let slave_attach context metadata host id writable is_reattach =
		fix_ctx context (Some id);
		if not is_reattach then check_all_hosts_present context metadata;
		debug "Got to the slave_attach function call";
		let resize_fn leaf_info =
			if leaf_info.attachment=None then
				match leaf_info.leaf with
					| PVhd vhduid ->
						let vhd = Vhd_records.get_vhd context metadata.m_data.m_vhds vhduid in
						ignore(Master_utils.resize_vhd context metadata vhd leaf_info.reservation_override (Some writable))
					| PRaw _ ->
						()
		in
		let leaf_info =
			if is_reattach
			then Locking.slave_attach_reattach context metadata host id writable (fun _ -> ())
			else Locking.slave_attach_lock context metadata host id writable resize_fn in
		Master_utils.get_slave_attach_info_inner context metadata id writable leaf_info

	let get_slave_attach_info context metadata id =
		fix_ctx context (Some id);
		let leaf_info = Locking.get_leaf_info metadata id in
		Master_utils.get_slave_attach_info_inner context metadata id true leaf_info		

	let slave_detach context metadata host id =
		fix_ctx context (Some id);
		let resize_fn leaf_info =
			if leaf_info.attachment=None then
				match leaf_info.leaf with
					| PVhd vhduid ->
						let vhd = Vhd_records.get_vhd context metadata.m_data.m_vhds vhduid in
						ignore(Master_utils.resize_vhd context metadata vhd leaf_info.reservation_override (Some false))
					| PRaw _ ->
						()
		in
		ignore(Locking.slave_attach_unlock context metadata host id resize_fn)

	let slave_activate context metadata host id is_reactivate =
		fix_ctx context (Some id);
		ignore(Locking.slave_activate context metadata host id is_reactivate)

	let slave_deactivate context metadata host id =
		fix_ctx context (Some id);
		ignore(Locking.slave_deactivate context metadata host id)

	let thin_provision_request_more_space context metadata host ids_and_physsizes =
          fix_ctx context None;
          let attach_infos = List.filter_map (fun vs_size_data -> 
	    let id = vs_size_data.vs_id in
	    let physsize = vs_size_data.vs_size in
            debug "Resizing vdi: %s (current physsize=%Ld)" id physsize;
            Locking.with_resize_lock context metadata id (fun leaf_info ->
              match leaf_info.leaf with
              | PVhd vhduid ->
                debug "Checking that this VDI (id:%s) is still attached to the correct host (%s)" id host;
                if (Locking.check_i_am_attached context metadata id host) then begin
                  let (location,size) = Nmutex.execute context metadata.m_vhd_hashtbl_lock "Need to get the 'location' field from the VHD tree"
                    (fun () ->
                      let vhd_info = Vhd_records.get_vhd context metadata.m_data.m_vhds vhduid in
                      let new_size = Vhdutil.update_phys_size vhd_info.size physsize in
                      Vhd_records.update_vhd_size context metadata.m_data.m_vhds vhduid new_size;
                      (vhd_info.location,new_size))
                  in
                  match Locking.with_container_write_lock context metadata (fun container ->
                                                          (* TODO: The next line is incorrect - physsize is the size with vhd overhead, but the function expects it without the overhead.
                                                             Could add a function to vhdutil to do the correct calculation (ie, min_phys_size=physsize) *)
		    let new_size = Vhdutil.size_with_thin_provisioning_overhead size in
		    debug "new_size=%Ld - update LVM metadata" new_size;
                    let container = Lvmabs.resize context container location new_size in
		    let new_attach_info = Lvmabs.get_attach_info context container location in
		    (match new_attach_info with | Some ai ->
		      debug "new_attach_info=%s" (Jsonrpc.to_string (Int_types.rpc_of_lv_attach_info_t ai)) | _ -> ());
                    (container,new_attach_info)) with
                  | _,Some (Mlvm ai) -> Some ai
                  | _ -> None
                end else begin
                  debug "Not resizing: The VDI is not attached!";
                  None
                end
              | PRaw x ->
                error "Can't thin provision a raw LVM volume!";
                None
            ))
            ids_and_physsizes
          in
          debug "Resized all";
          attach_infos
	    
	let create context metadata vdi_info =
	  let virtual_size = vdi_info.virtual_size in
	  let sm_config = vdi_info.sm_config in

		fix_ctx context None;
		let location_uuid = Uuidm.to_string (Uuidm.create Uuidm.(`V4)) in

		let raw =
			if List.mem_assoc "type" sm_config
			then List.assoc "type" sm_config = "raw"
			else false
		in

		let id = Uuidm.to_string (Uuidm.create Uuidm.(`V4)) in

		let (ptr,vsize,lvsize) =
			if raw
			then begin
				let vsize = Vhdutil.roundup virtual_size Lvm.Constants.extent_size in
				let lvsize = vsize in
				let container,location =
					Locking.with_container_write_lock context metadata (fun container ->
						let container,location = Lvmabs.create_raw context container location_uuid lvsize in
						let container = Lvmabs.commit context container in
						(container, location))
				in
				(PRaw location, vsize, lvsize)
			end else begin
				let size = Vhdutil.get_size_for_new_vhd virtual_size in
				let vsize = Vhdutil.get_virtual_size size in
				let lvsize = Vhdutil.get_lv_size metadata.m_data.m_lvm_reservation_mode (Some false) size in
				let container,location =
					Locking.with_container_write_lock context metadata (fun container ->
						let container,location = Lvmabs.create context container location_uuid lvsize in
						let container = Lvmabs.commit context container in
						(container, location))
				in

				Html.signal_master_metadata_change metadata ();

				let uid = Lvmabs.with_active_vhd context container location false (fun _ nod ->
					Vhd.create nod vsize (Vhd.Ty_dynamic) (Vhdutil.max_size) [];
					let vhd=Vhd._open nod [Vhd.Open_rdonly] in
					let uid = Vhd.get_uid vhd in
					Vhd.close vhd;

					Vhd_records.add_vhd context metadata.m_data.m_vhds uid  
						{vhduid=uid;
						parent=None;
						location=location;
						size=Vhdutil.query_size nod;
						hidden=0;
						};
					
					uid) in

				Html.signal_master_metadata_change metadata ();

				(PVhd uid, vsize, lvsize)
			end
		in

		let smapiv2_info = Vhd_types.({
		  content_id="";
		  name_label=vdi_info.Storage_interface.name_label;
		  name_description=vdi_info.Storage_interface.name_description;
		  ty=vdi_info.Storage_interface.ty;
		  metadata_of_pool=vdi_info.Storage_interface.metadata_of_pool;
		  is_a_snapshot=false;
		  snapshot_time="";
		  snapshot_of="";
		  read_only=false;
		  persistent=true;
		  sm_config=vdi_info.Storage_interface.sm_config;
		}) in

		Id_map.add_to_id_map context metadata id ptr None smapiv2_info;

		{vdi_info with vdi=id;
		  is_a_snapshot=false;
		  snapshot_time=Date.to_string Date.never;
		  snapshot_of="OpaqueRef:NULL";
		  read_only=false;
		  physical_utilisation=0L;
		}

	let update context metadata gp vdi =
		let id = vdi in
		fix_ctx context (Some id);
		(* Update the virtual_size, physical_utilisation, read_only flag and sm_config map *)
		
		()

	let vhd_info_of_leaf ctx dbg metadata container (id,leaf_info) =
	  let phys_size,virt_size = match leaf_info.leaf with
	    | PVhd vhduid ->
	      let vhd = Vhd_records.get_vhd ctx metadata.m_data.m_vhds vhduid in
	      let virtual_size = Vhdutil.get_virtual_size vhd.size in
	      let phys_size = fst (Lvmabs.size ctx container vhd.location) in
	      phys_size, virtual_size
	    | PRaw location_info ->
	      let phys_size = fst (Lvmabs.size ctx container location_info) in
	      phys_size, phys_size
	  in

	  Storage_interface.({ vdi=id;
	    content_id       = leaf_info.smapiv2_info.Vhd_types.content_id;
	    name_label       = leaf_info.smapiv2_info.Vhd_types.name_label;
	    name_description = leaf_info.smapiv2_info.Vhd_types.name_description;
	    ty               = leaf_info.smapiv2_info.Vhd_types.ty;
	    metadata_of_pool = leaf_info.smapiv2_info.Vhd_types.metadata_of_pool;
	    is_a_snapshot    = leaf_info.smapiv2_info.Vhd_types.is_a_snapshot;
	    snapshot_time    = leaf_info.smapiv2_info.Vhd_types.snapshot_time;
	    snapshot_of      = leaf_info.smapiv2_info.Vhd_types.snapshot_of;
	    read_only        = leaf_info.smapiv2_info.Vhd_types.read_only;
	    virtual_size     = virt_size;
	    physical_utilisation = phys_size;
	    persistent       = leaf_info.smapiv2_info.Vhd_types.persistent;
	    sm_config        = leaf_info.smapiv2_info.Vhd_types.sm_config; })
		  
	let stat ctx dbg metadata vdi =
	  let id = vdi in
	  fix_ctx ctx (Some id);
	  let leaf_info = Locking.locked_get_leaf_info ctx metadata id in
	  let container = Locking.with_container_read_lock ctx metadata (fun () -> metadata.m_data.m_vhd_container) in
	  vhd_info_of_leaf ctx dbg metadata container (id,leaf_info)

	let set_persistent ctx dbg metadata vdi persistent =
	  let id = vdi in
	  fix_ctx ctx (Some id);
	  Id_map.update_smapiv2_info ctx metadata id (fun smapiv2_info ->
	    Vhd_types.({smapiv2_info with persistent=persistent}))

	let compose ctx dbg metadata vdi1 vdi2 =
	  (* VDI1 contains some diffs, and VDI2 contains the base copy. This sets
	     VDI1's parent to be VDI2 and deletes VDI2 *)
	  let id = vdi1 in
	  fix_ctx ctx (Some id);
	  check_all_hosts_present ctx metadata;

(*	  Locking.with_delete_lock context metadata vdi2 (fun leaf_info ->
	    if leaf_info.attachment <> None then failwith "Can't compose onto an attached VDI";
	    let ptr = leaf_info.leaf in
	    match ptr with
	    | PVhd vhduid ->
	      Id_map.remove_id_from_map context metadata id;

	      let vhd = Vhd_records.get_vhd context metadata.m_data.m_vhds vhduid in
	      let (hidden,size) = Master_utils.set_hidden context metadata vhd in
	      Vhd_records.update_hidden context metadata.m_data.m_vhds (PVhd vhduid) hidden;
	      let vhdrec = Vhd_records.remove_vhd context metadata.m_data.m_vhds vhduid in

	      Html.signal_master_metadata_change metadata ();

	      ignore(Locking.with_container_write_lock context metadata (fun container ->
		let container = Lvmabs.remove context container vhdrec.location in
		(Lvmabs.commit context container,())));

	      Html.signal_master_metadata_change metadata ()


	    | PRaw x ->
	      failwith "Not implemented"
	  )*)
	  failwith "Not implemented"


	let add_to_sm_config ctx dbg metadata vdi key value =
	  let id = vdi in
	  fix_ctx ctx (Some id);
	  Id_map.update_smapiv2_info ctx metadata id (fun smapiv2_info ->
	    Vhd_types.({ smapiv2_info with
	      sm_config = (key,value)::(List.filter (fun (x,y) -> x <> key) smapiv2_info.sm_config) })
	  )

	let remove_from_sm_config ctx dbg metadata vdi key =
	  let id = vdi in
	  fix_ctx ctx (Some id);
	  Id_map.update_smapiv2_info ctx metadata id (fun smapiv2_info ->
	    Vhd_types.({ smapiv2_info with
	      sm_config = List.filter (fun (x,y) -> x <> key) smapiv2_info.sm_config })
	  )

	let set_content_id ctx dbg metadata vdi content_id =
	  let id = vdi in
	  fix_ctx ctx (Some id);
	  Id_map.update_smapiv2_info ctx metadata id (fun smapiv2_info ->
	    Vhd_types.({ smapiv2_info with
	      content_id = content_id }))

	let delete context metadata vdi =
		let id = vdi in
		fix_ctx context (Some id);
		check_all_hosts_present context metadata;

		Locking.with_delete_lock context metadata id (fun leaf_info ->
			if leaf_info.attachment <> None then failwith "Can't delete an attached VDI";
			let ptr = leaf_info.leaf in
			match ptr with
				| PVhd vhduid ->
					Id_map.remove_id_from_map context metadata id;

					let vhd = Vhd_records.get_vhd context metadata.m_data.m_vhds vhduid in
					let (hidden,size) = Master_utils.set_hidden context metadata vhd in
					Vhd_records.update_hidden context metadata.m_data.m_vhds (PVhd vhduid) hidden;
					let vhdrec = Vhd_records.remove_vhd context metadata.m_data.m_vhds vhduid in

					Html.signal_master_metadata_change metadata ();

					ignore(Locking.with_container_write_lock context metadata (fun container ->
						let container = Lvmabs.remove context container vhdrec.location in
						(Lvmabs.commit context container,())));

					Html.signal_master_metadata_change metadata ()


				| PRaw x ->
					failwith "Not implemented"
		)

	exception Parent_missing


	let clone_inner new_reservation_override context metadata vdi =
		let id = vdi.Storage_interface.vdi in
		fix_ctx context (Some id);

		if metadata.m_data.m_rolling_upgrade then 
			failwith "Can't clone during rolling upgrade";
		debug "Clone: Checking all hosts present";
		check_all_hosts_present context metadata;
		debug "OK";

		let result = Clone.clone context metadata vdi new_reservation_override in
		result

	let clone context metadata vdi = clone_inner None context metadata vdi 

	let snapshot context metadata vdi = clone_inner (Some Vhdutil.Attach) context metadata vdi

	let resize context metadata vdi newsize =
		let id = vdi in
		fix_ctx context (Some id);
		let vsize = Vhdutil.roundup newsize Lvm.Constants.extent_size in
	
		if metadata.m_data.m_rolling_upgrade then 
			failwith "Can't resize during rolling upgrade";

		Locking.with_resize_lock context metadata id (fun leaf_info ->
			match leaf_info.leaf with
				| PVhd x -> 
					let vhd = Vhd_records.get_vhd context metadata.m_data.m_vhds x in
					let container = Locking.get_container context metadata in
					let new_size = Lvmabs.with_active_vhd context container vhd.location false (fun ty nod ->
						Vhdutil.with_vhd nod true (fun vhd ->
							debug "About to set_virt_size";
							Vhd.set_virt_size vhd vsize;
							debug "About to query size";
							Vhdutil.query_size_vhd vhd)) in
					let new_vhd_record = Vhd_records.update_vhd_size context metadata.m_data.m_vhds x new_size in
					ignore(Master_utils.resize_vhd context metadata new_vhd_record leaf_info.reservation_override (Some false))
				| PRaw loc ->
					ignore(Locking.with_container_write_lock context metadata (fun container -> 
						(Lvmabs.resize context container loc vsize, ())))
		);
						
		Master_utils.reattach context metadata id;

		newsize

	let resize_online ctx metadata vdi newsize =
		resize ctx metadata vdi newsize

	let do_leaf_coalesce context metadata id =
		fix_ctx context (Some id);
		Leaf_coalesce.leaf_coalesce context metadata id

	let leaf_coalesce context metadata device_config vdi =
		let id = vdi in
		fix_ctx context (Some id);
		(* Grab the coalesce lock first *)

		Coalesce.with_coalesce_lock context metadata (fun () ->
			do_leaf_coalesce context metadata id)


	let external_clone ctx metadata filename =
		failwith "Not implemented"
end


			   module SR = struct

	(* The attach proceeds as follows:

	   - It first determines the type of driver (Lvm,OldLvm or File) and
	   initialises the LVM abstraction layer appropriately. It then
	   checks to make sure the UUID is correct.

	   - It then scans for VHDs, which loads in the important info from
	   each VHD it finds (this can take a few seconds).

	   - It then locates the metadata - the id-to-leaf map and the host
	   attachments - and loads in the info. If the metadata doesn't exist,
	   we assume we're upgrading from old LVHD and recreate the metadata
	   based upon uuids. If the metadata does exist but is unreadable, we
	   fail.

	   - Any inconsistencies are now resolved. If there are any VHDs that
	   are not reachable starting from the leaves, then these are removed.
	   If there are any ids that don't point at a VHD, then these are removed.
	   Nb. It's likely that we'll have to move this to a point after the
	   slaves have resynced their data to ensure we don't remove anything
	   that is currently in use.

	   - The reservation policy is then determined. Note that switching
	   between reservation policies is not implemented, but could be
	   done as part of the post-slave-recover sync.
	*)

			     let attach context device_config driver path sr =
			       fix_ctx context None;

			       let m_rolling_upgrade = List.mem_assoc "rolling_upgrade_mode" device_config in
			       debug "m_rolling_upgrade=%b" m_rolling_upgrade;

			       let check container =
				 let container_sr_uuid = Lvmabs.container_sr_uuid context container in
				 if container_sr_uuid <> sr then begin
				   debug "container_sr_uuid=%s expecting:%s" container_sr_uuid sr;
				   failwith "Could't find VG"
				 end
			       in

			       let (container,vhds,raw_lvs) =
				 let container =
				   match driver with
				   | Lvm _ ->
				     let device = path in
				     Lvmabs.init_lvm context (String.split ',' device)
				   | OldLvm _ ->
				     let device = path in
				     Lvmabs.init_origlvm context sr (String.split ',' device)
				   | File _ ->
				     let path = path in
				     Lvmabs.init_fs context path
				 in
				 debug "container initialised";
				 check container;
				 let vhds = Scan.scan context container in
				 let raw_lvs =
				   let hidden_lvs = Lvmabs.get_hidden_lvs context container in
				   Lvmabs.scan context container (fun loc_info ->
				     if loc_info.Lvmabs_types.location_type=Lvmabs_types.Raw 
				     then Some (PRaw (loc_info),List.mem loc_info hidden_lvs)
				     else None)
				 in
				 (container,vhds,raw_lvs)
			       in
			       
			       let (container,host_attachments_persistent_store) =
				 match Lvmabs.find_metadata context container "host_attachments" with
				 | Some x -> x
				 | None -> 
				   try 
				     Lvmabs.create_metadata context container "host_attachments" true
				   with e -> 
				     error "Caught error while trying to create host_attachments metadata: %s" (Printexc.to_string e);
				     failwith "Error attaching"
			       in

		(* No other masters check *)

			       begin
				 let attachments_info =
				   Slave_sr_attachments.read_slave_sr_attachment_info_from_disk context container host_attachments_persistent_store
				 in
				 match attachments_info.master with
				 | Some h ->
				   if h.h_uuid <> Global.get_host_uuid () then begin
						(* If we're forcibly taking over mastership from someone else, we need to make
						   sure it's not still the master *)
				     debug "Found that the SR believes another host is the master. Attempting to query";
				     let module Client = (val (Int_client.get h) : Int_client.CLIENT) in

				     let ok =
				       try
					 let old_master_mode = Client.SR.mode ~sr in
					 debug "old_master_mode = %s" (Jsonrpc.to_string (rpc_of_attach_mode old_master_mode));
					 old_master_mode <> Master
				       with e ->
					 debug "Caught exception while trying to contact old master: %s" (Printexc.to_string e);
					 debug "Assuming that it's no longer the master then!";
					 true
				     in
				     if not ok then
				       raise (Other_master_detected h)
				   end
				 | None -> ()
			       end;

			       let container,id_persistent_store_option,do_init = 
				 match Lvmabs.find_metadata context container "id_to_leaf_mapping" with
				 | Some (container,location) -> (container,Some location,false)
				 | None -> 
				   try 
				     let (container,location) = Lvmabs.create_metadata context container "id_to_leaf_mapping" false in
				     (container,Some location,true)
				   with _ ->
				     (container,None,true)
			       in
			       let id_map = Id_map.initialise_id_map context container id_persistent_store_option vhds do_init in

			       let reservation_mode =
				 match driver with
				 | File _ -> None
				 | _ ->
				   try
				     match List.assoc "reservation_mode" device_config with
				     | "thin" -> Some Vhdutil.Thin
				     | "leaf" -> Some Vhdutil.Leaf
				     | "attach" -> Some Vhdutil.Attach
				     | _ -> Some Vhdutil.Leaf
				   with _ -> Some Vhdutil.Leaf
			       in

			       debug "Selected provisioning policy: %s"
				 (match reservation_mode with
				 | None -> "None"
				 | Some Vhdutil.Leaf -> "Leaf"
				 | Some Vhdutil.Thin -> "Thin"
				 | Some Vhdutil.Attach -> "Attach" );

		(* Sanity check *)
			       begin
				 if m_rolling_upgrade then
				   match reservation_mode with
				   | None -> ()
				   | Some Vhdutil.Leaf -> ()
				   | _ -> failwith "Only leaf provisioning mode supported during rolling upgrade";
			       end;

			       let m_vhds = Vhd_records.of_vhds vhds raw_lvs in

			       let attached_hosts = Slave_sr_attachments.read_attached_hosts_from_disk context container host_attachments_persistent_store in
			       Tracelog.append context (Tracelog.Master_m_vhd_container container) (Some "Initial value");
			       Tracelog.append context (Tracelog.Master_m_id_to_leaf_mapping (Hashtbl.copy id_map)) (Some "Initial value");
			       Tracelog.append context (Tracelog.Master_m_vhds m_vhds) (Some "Initial value");
			       Tracelog.append context (Tracelog.Master_m_attached_hosts attached_hosts) (Some "Initial value");
			       Tracelog.append context (Tracelog.Master_m_coalesce_in_progress false) (Some "Initial value");
			       Tracelog.append context (Tracelog.Master_m_rolling_upgrade m_rolling_upgrade) (Some "Initial value");
			       { 
				 m_data = 
				   { 
				     m_vhd_container=container;
				     m_id_mapping_persistent_store=container,id_persistent_store_option;
				     m_id_to_leaf_mapping=id_map;
				     m_vhds=m_vhds;
				     m_attached_hosts_persistent_store=(container,host_attachments_persistent_store);
				     m_attached_hosts=attached_hosts;
				     m_coalesce_in_progress=false;
				     m_lvm_reservation_mode=reservation_mode;
				     m_rolling_upgrade=m_rolling_upgrade;
				     m_attach_finished=false;
				     m_sr_uuid=sr };
				 m_idx=0;
				 m_container_lock=Rwlock.create ();
				 m_id_mapping_lock=Nmutex.create "m_id_mapping_lock";
				 m_id_mapping_condition=Nmutex.create_condition "m_id_mapping_condition";
				 m_vhd_hashtbl_lock=Nmutex.create "m_vhd_hashtbl_lock";
				 m_attached_hosts_lock = Nmutex.create "m_attached_hosts_lock";
				 m_attached_hosts_condition = Nmutex.create_condition "m_attached_hosts_condition";
				 m_coalesce_in_progress_lock=Nmutex.create "m_coalesce_in_progress_lock";
				 m_coalesce_condition=Nmutex.create_condition "m_coalesce_condition";
			       }

	let attach_part_two context metadata =
		fix_ctx context None;
		debug "Attach part two";
		let leaf_infos = Locking.get_all_leaf_infos context metadata in
		(* Resolve map inconsistencies - remove VHDs for which there are no IDs, and remove IDs for which there are no VHDs *)
		debug "Resolving map inconsistencies";
		List.iter (fun (id,_) ->
			Locking.with_delete_lock context metadata id (fun leaf_info ->
				let leaf_present = match leaf_info.leaf with
					| PVhd vhduid ->
						(try ignore(Vhd_records.get_vhd context metadata.m_data.m_vhds vhduid); true with _ -> false)
					| PRaw loc ->
						Lvmabs.exists context (Locking.with_container_read_lock context metadata (fun () -> metadata.m_data.m_vhd_container)) loc
				in
				if not leaf_present then begin
					warn "Warning: Removing stale entry from the ID to leaf map: %s (leaf %s not found)" id (string_of_pointer leaf_info.leaf);
					Id_map.remove_id_from_map context metadata id
				end
			)) leaf_infos;
		debug "Resolved";
		try
			Coalesce.with_coalesce_lock context metadata (fun () -> () (*Coalesce.coalesce context metadata true*) );
			metadata.m_data.m_attach_finished <- true
		with e -> 
			debug "Ack! Caught exception: %s" (Printexc.to_string e);
			raise e

	(* Here we resync any slaves that are marked as needing it. When they're all done, do attach part two. Guarantees
	   not to block. *)
	let recover_slaves context metadata =
		fix_ctx context None;
		debug "Recovering any slaves";
		let attached_hosts = Slave_sr_attachments.get_attached_hosts context metadata in
		debug "About to get_localhost()";
		let localhost = Global.get_localhost () in
		let need_resync = List.filter (fun ssa -> ssa.ssa_resync_required) attached_hosts in
		debug "Creating the attach_part_two thread";

		ignore(Thread.create (fun () ->			
			debug "attach_part_two thread created";
			Nmutex.execute context metadata.m_attached_hosts_lock "Checking whether resync is required" (fun () ->
				while List.fold_left (fun acc ssa -> ssa.ssa_resync_required || acc) 
					metadata.m_data.m_rolling_upgrade attached_hosts do
					Nmutex.condition_wait context metadata.m_attached_hosts_condition metadata.m_attached_hosts_lock
				done);
			attach_part_two context metadata) ());					
		debug "About to iterate over attached slaves";

		List.iter (fun ssa ->
			ignore(Thread.create (fun () ->
				debug "Recovering slave %s" ssa.ssa_host.h_uuid;
				Int_client_utils.master_retry_loop context [e_sr_not_attached] [] (fun client ->
				  let module Client = (val client : Int_client.CLIENT) in
				  debug "Issuing slave recover";
				  Client.SR.slave_recover ~sr:metadata.m_data.m_sr_uuid ~master:localhost;
				  debug "Done";
				  Nmutex.execute context metadata.m_attached_hosts_lock "Setting resync_required state" (fun () ->
				    ssa.ssa_resync_required <- false;
				    Nmutex.condition_broadcast context metadata.m_attached_hosts_condition);
				) metadata ssa.ssa_host) ())) need_resync
		  
	let assert_can_detach context metadata device_config =
		if metadata.m_data.m_attach_finished <> true then 
			failwith "Can't detach when not finished attaching"

	let detach context metadata device_config =
		fix_ctx context None;
		Slave_sr_attachments.commit_attached_hosts_to_disk context metadata None; (* Commit to detaching! *)
		Lvmabs.shutdown context metadata.m_data.m_vhd_container

	let create context driver path device_config sr size =
		fix_ctx context None;
		match driver with
			| Drivers.Lvm _ ->
				let vg_name = "VG_XenStorage-"^sr in
				let devices = String.split ',' path in
				let _,devices_and_names = List.fold_right (fun dev (i,cur) -> (i+1,(dev,Printf.sprintf "pv%d" i)::cur)) devices (0,[]) in
				let _ = Lvm.Vg.create_new vg_name devices_and_names in
				()
			| Drivers.OldLvm _ ->
				let vg_name = "VG_XenStorage-"^sr in
				let devices = String.split ',' path in
				Olvm.create_vg sr vg_name devices
			| Drivers.File _ ->
				()

	let delete context metadata device_config sr path = failwith "Implemented elsewhere"			

	(* The probe here is only run once the transport has been attached *)
	let probe context driver device_config path =
		fix_ctx context None;
		match driver with
			| Lvm _ ->
				let device = path in
				let probed_srs = 
				  try
				    debug "device=%s" device;
				    let container = Lvmabs.init_lvm context (String.split ',' device) in
				    let container_sr_uuid = Lvmabs.container_sr_uuid context container in
				    Lvmabs.shutdown context container;
				    [container_sr_uuid]
				  with _ ->
				    []
				in
				Storage_interface.({probed_srs; probed=[]; other_data=None})
			| OldLvm _ ->
				let device = path in
				let probed_srs = 
				  try
				    debug "device=%s" device;
				    let container = Lvmabs.init_origlvm context "probe" (String.split ',' device) in
				    let container_sr_uuid = Lvmabs.container_sr_uuid context container in
				    Lvmabs.shutdown context container;
				    [container_sr_uuid]
				  with _ ->
				    []
				in
				Storage_interface.({probed_srs; probed=[]; other_data=None})
			| File _ ->
			  failwith "Unimplemented"

	(* TODO: Fix up xapi's database. This currently only kicks off the coalesce process *)
	let scan context dbg sr =
		fix_ctx context None;
		let metadata = Attachments.gmm sr in
		if not metadata.m_data.m_rolling_upgrade then begin
			Coalesce.with_coalesce_lock context metadata (fun () -> 
				let contents = classify_sr_contents context metadata in
				debug "classify_sr_contents:";
				debug "coalescable_vhds: [%s]" (String.concat ";" contents.coalescable_vhds);
				debug "leaf_coalescable_ids: [%s]" (String.concat ";" contents.leaf_coalescable_ids);
				debug "unreachable_vhds: [%s]" (String.concat ";" (List.map (fun (k,v) -> Printf.sprintf "(%s,%b)" k v) contents.unreachable_vhds));
				debug "unreachable_lvs: [%s]" (String.concat ";" (List.map (fun (k,v) -> Printf.sprintf "(%s,%b)" (Lvmabs.string_of_location_info k) v) contents.unreachable_lvs));
				debug "broken_vhds: [%s]" (String.concat ";" contents.broken_vhds);
				debug "bad_parents: [%s]" (String.concat ";" (List.map (function PVhd x -> Printf.sprintf "PVhd (%s)" x | PRaw lv -> Printf.sprintf "PRaw (%s)" (Lvmabs.string_of_location_info lv)) contents.bad_parents));
				List.iter (fun vhduid -> ignore(Coalesce.resize_and_move_data context metadata None None vhduid)) contents.coalescable_vhds;
				List.iter (Coalesce.relink_children context metadata) contents.coalescable_vhds;
				List.iter (fun (loc,_) ->
					ignore(Locking.with_container_write_lock context metadata (fun container -> (Lvmabs.remove context container loc,())))) contents.unreachable_lvs;
				List.iter (fun (vhduid,_) ->
					let vhd = Vhd_records.remove_vhd context metadata.m_data.m_vhds vhduid in
					ignore(Locking.with_container_write_lock context metadata (fun container -> (Lvmabs.remove context container vhd.location, ())))) contents.unreachable_vhds;
				List.iter (fun vdi -> try ignore(VDI.do_leaf_coalesce context metadata vdi) with Leaf_coalesce.Cant_leaf_coalesce _ -> ()) contents.leaf_coalescable_ids)
		end;

		let leaf_infos = Locking.get_all_leaf_infos context metadata in
		let container = Locking.with_container_read_lock context metadata (fun () -> metadata.m_data.m_vhd_container) in
		List.map (VDI.vhd_info_of_leaf context dbg metadata container) leaf_infos

	let update context driver sr = 
		fix_ctx context None;
		let metadata = Attachments.gmm sr in
		ignore metadata

	let slave_attach context metadata tok host ids =
		fix_ctx context None;
		debug "Attach from host: %s, ip: %s" (host.h_uuid) (match host.h_ip with Some x -> x | None -> "unknown");
		Slave_sr_attachments.slave_sr_attach context metadata host ids

	let slave_detach context metadata tok host =
		fix_ctx context None;
		debug "Detach from host: %s, ip: %s" (host.h_uuid) (match host.h_ip with Some x -> x | None -> "unknown");
		Slave_sr_attachments.slave_sr_detach context metadata host

	let set_host_dead context metadata host_uuid =
		fix_ctx context None;
		Slave_sr_attachments.slave_sr_detach_by_host_uuid context metadata host_uuid;
		let leaves = Locking.get_all_leaf_infos context metadata in
		List.iter (fun (k,leaf_info) ->
			try VDI.slave_detach context metadata host_uuid k with Int_rpc.IntError(e_not_attached,_) -> ()) leaves

	let set_rolling_upgrade_finished context metadata =
		fix_ctx context None;
		Nmutex.execute context metadata.m_attached_hosts_lock "Broadcasting the end of rolling upgrade mode" (fun () ->
			metadata.m_data.m_rolling_upgrade <- false;
			Nmutex.condition_broadcast context metadata.m_attached_hosts_condition)

end
