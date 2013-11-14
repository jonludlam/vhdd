(* The SMAPI dispatcher module *)

open Int_types
open Threadext
open Stringext
open Storage_interface

type context = Context.t

module D = Debug.Make(struct let name="vhdsm" end)
open D

module DP = struct include Storage_skeleton.DP end

module Query = struct
  let query ctx ~dbg =
    let open Context in
    { driver = ctx.c_driver;
      name = ctx.c_driver;
      description = "Daemonized VHD SR";
      vendor = "Citrix";
      copyright = "Citrix";
      version = "2";
      required_api_version = "1.1";
      features = [
	"SR_PROBE";
	"SR_UPDATE";
	"VDI_CREATE";
	"VDI_DELETE";
	"VDI_ATTACH";
	"VDI_DETACH";
	"VDI_RESIZE";
	"VDI_RESIZE_ONLINE";
	"VDI_CLONE";
	"VDI_SNAPSHOT";
	"VDI_ACTIVATE";
	"VDI_DEACTIVATE";
	"VDI_UPDATE";
	"VDI_INTRODUCE";
      ] @ (if Drivers.supports_ha ctx.c_driver then ["VDI_GENERATE_CONFIG"] else []);
      configuration = Drivers.get_driver_config ctx.c_driver;
    }
    let diagnostics ctx ~dbg = "Not available"
  end

module UPDATES = struct include Storage_skeleton.UPDATES end
module TASK = struct include Storage_skeleton.TASK end
module Policy = struct include Storage_skeleton.Policy end
module DATA = struct include Storage_skeleton.DATA end
let get_by_name = Storage_skeleton.get_by_name

module SR = struct
	(* TODO: return more interesting configuration information *)

	let probe ctx gp sr_sm_config =
		let driver = Drivers.of_ctx ctx in
		Transport.probe driver gp (fun path ->
			VhdMaster.SR.probe ctx driver gp sr_sm_config path)

	let create ctx ~dbg ~sr ~device_config ~physical_size =
		(* Fail if we've already got an SR with the same uuid attached *)
		let exists = (try ignore(Attachments.gmm sr); true with _ -> false) || (try ignore(Attachments.gsm sr); true with _ -> false) in

		if exists then failwith "An SR with the specified UUID is already attached";

		let driver = Drivers.of_ctx ctx in
 		try
			let path = Transport.attach driver sr device_config true in
			VhdMaster.SR.create ctx driver path device_config sr physical_size;
			Transport.detach driver sr device_config;
		with Transport.MissingParam name ->
		        raise (Missing_configuration_parameter name)

	let scan ctx ~dbg ~sr = VhdMaster.SR.scan ctx dbg sr

	let update ctx sr = VhdMaster.SR.update ctx (Drivers.of_ctx ctx) sr

	let mode ctx sr_uuid =
		let sr = sr_uuid in
		let slave_metadata = try Some (Attachments.gsm sr) with _ -> None in
		let master_metadata = try Some (Attachments.gmm sr) with _ -> None in
		match master_metadata, slave_metadata with
			| Some _, Some _ -> Master
			| None, Some s -> Slave s.Vhd_types.s_data.Vhd_types.s_master
			| _ -> failwith "Not attached"


	let maybe_add_pv_info ctx driver path sr =
		let container =
			match driver with
				| Drivers.Lvm _ ->
					Lvmabs.init_lvm ctx (String.split ',' path)
				| Drivers.OldLvm  _ ->
					Lvmabs.init_origlvm ctx sr (String.split ',' path)
				| Drivers.File _ ->
					Lvmabs.init_fs ctx path
		in
		Lvmabs.maybe_add_pv_ids ctx sr container

	(* Here we do the attach-as-a-slave and attach-as-a-master. For
	   the master case, we attach as a master first, since the slave
	   will need to register with the master to ask permission to
	   attach. It also may call to reattach any currently-attached
	   VDIs to sync up their metadata. Once the attachment is
	   complete, we perform resync operations (recover_slaves and
	   slave_recover) *)
	let attach_in_mode ctx device_config path sr mode is_reattach =
		let driver = Drivers.of_ctx ctx in
		let slave_conf = Attachments.gsm sr in
		begin
			match mode with
				| Master ->
					let master_conf = VhdMaster.SR.attach ctx device_config driver path sr in
					Attachments.attach_as_master sr master_conf;
					Html.signal_master_metadata_change master_conf ();

					slave_conf.Vhd_types.s_rpc <- (!Vhdrpc.local_rpc);

					(* Now, if we had logged any slaves as being attached, recover them *)
					VhdMaster.SR.recover_slaves ctx master_conf;

					VhdSlave.SR.register_with_master ctx slave_conf (Global.get_localhost ());
					
					debug "s_ready for the slave is: %b" (Attachments.gsm sr).Vhd_types.s_data.Vhd_types.s_ready;

					Slave_sr_attachments.commit_attached_hosts_to_disk ctx master_conf (Some (Global.get_localhost ()))

				| Slave (Some host) ->

					slave_conf.Vhd_types.s_rpc <- (fun task -> Vhdrpc.remote_rpc task (match host.h_ip with Some x -> x | None -> "Unknown")  host.h_port);

					let reg () =
						VhdSlave.SR.register_with_master ctx slave_conf host;
						VhdSlave.SR.slave_recover ctx slave_conf "foo" host
					in
					if is_reattach then
						ignore(Thread.create reg ())
					else
						reg ()

				| Slave None ->
					failwith "Can't do this bit of the attach without a master"
		end;
		Attachments.log_attachment 
			{Attachments.drivertype=Drivers.string_of driver;
			path=path;
			uuid=sr;
			mode=mode;
			device_config}


	(* The attach function first attaches the underlying block device via
	   the transport module, then determines master/slave mode.

	   Master mode is selected if ("SRmaster","true") is passed in the
	   device-config map. If this is not present, slave mode is
	   selected. If slave mode is selected, the daemon must determine
	   the IP/port of the master. This is currently done in one of two
	   ways. If ("SRmasterip",ip) is passed in the device-config then
	   this is used, otherwise it probes for the master by attempting to
	   read the 'host_attachments' metadata file/LV. It tries once a
	   second for a maximum of 60 seconds before giving up.

	   Once this is determined, we see if we're already attached in some
	   mode, in which case we might go into some change-of-mode logic
	   (currently unimplemented). Otherwise, we call attach_real.  *)


	(* Ensures that the transport layer is attached, then calls the passed
	   function with the path *)
	let with_transport_path ctx device_config sr f =
		let driver = Drivers.of_ctx ctx in

		(* Check if the transport layer is already attached *)
		let slave_metadata = try Some (Attachments.gsm sr) with _ -> None in

		(* Retrieve the path from the slave metadata, or attach the transport layer *)
		match slave_metadata with
			| Some s -> f s.Vhd_types.s_data.Vhd_types.s_path
			| None ->
				let path = Transport.attach driver sr device_config false in
				try
					(* If we've attached the transport, register any physical volumes
					   that may be on the block device *)
					maybe_add_pv_info ctx driver path sr;

					(* Attach the slave module in no-master mode. This is a place to
					   stash the path info for later, and is also useful for 
					   attach-from-config *)
					let slave_conf = VhdSlave.SR.attach ctx path sr in
					Attachments.attach_as_slave sr slave_conf;
					Html.signal_slave_metadata_change slave_conf ();
					
					
					(* Log that we've attached - this is effectively saying that we've
					   only got as far as attaching the transport layer. Later on it
					   will be replaced with the mode in which we've attached *)
					Attachments.log_attachment 
						{Attachments.drivertype=Drivers.string_of driver;
						path=path;
						uuid=sr;
						mode=(Slave None);
						device_config};
					
					(* Jump to the continuation *)
					f slave_conf.Vhd_types.s_data.Vhd_types.s_path
				with e ->
					debug "Executing cleanup functions";
					Host.remove_pv_id_info sr;
					Transport.detach driver sr device_config;
					raise e

	(* Figure out whether we should be attaching in master or slave mode. If we're
	   slave mode we need to figure out where the master is. This logic is in 
	   master_probe *)
	let determine_mode ctx device_config sr path =
		let driver = Drivers.of_ctx ctx in
		let mode =
			try
				if List.mem_assoc "SRmaster" device_config &&
					List.assoc "SRmaster" device_config = "true" then
						Master
				else
					let host = Master_probe.master_probe ctx driver path sr in
					Slave (Some host)
			with e ->
				log_backtrace ();
				debug "Caught exception: %s" (Printexc.to_string e);
				failwith "Unable to determine master/slave mode"
		in

		debug "mode=%s"
			(match mode with
				| Master -> "Master"
				| Slave (Some host) -> Printf.sprintf "Slave (%s,%s,%d)" host.h_uuid (match host.h_ip with Some x -> x | None -> "Unknown") host.h_port
				| Slave None -> Printf.sprintf "Slave nomaster");
		mode

	let attach ctx ~dbg ~sr ~device_config =
		with_transport_path ctx device_config sr (fun path ->
			let mode = determine_mode ctx device_config sr path in
			attach_in_mode ctx device_config path sr mode false)

	let reattach sr =
		let ctx = Context.({c_driver=sr.Attachments.drivertype; c_api_call="sr_attach"; c_task_id=Uuidm.to_string (Uuidm.create Uuidm.(`V4)); c_other_info=[]}) in
		let driver = Drivers.of_string sr.Attachments.drivertype in
		let device_config = sr.Attachments.device_config in
		let path = sr.Attachments.path in
		let mode = sr.Attachments.mode in
		let sr' = sr.Attachments.uuid in
		maybe_add_pv_info ctx driver path sr';

		try
			let slave_conf = VhdSlave.SR.attach ctx path sr' in
			Attachments.attach_as_slave sr' slave_conf;
			Html.signal_slave_metadata_change slave_conf ();
			begin
				try
					attach_in_mode ctx device_config path sr' mode true
				with
					| VhdMaster.Other_master_detected m ->
						(* It's pretty important that we reattach in any way we can, since we might
						   have VDIs that are currently attached/activated. We mustn't lose track
						   of these, so in the case where we were a master before, and we detect
						   a new master, we reattach as a slave. Note that xapi might get out
						   of sync if we do this. *)
						begin
							try
								attach_in_mode ctx device_config path sr' (Int_types.Slave (Some m)) true;
								Attachments.log_attachment_new_master sr.Attachments.uuid (Some m)
							with e ->
								log_backtrace ();
								error "Caught error in reattaching! this is bad!";
								raise e
						end
			end
		with | e ->
			error "Ack! Got an exception while reattaching. Please fix me! '%s'" (Printexc.to_string e);
			log_backtrace ()

	let attach_nomaster ctx device_config sr =
		with_transport_path ctx device_config sr (fun path ->
			())

	let detach ctx ~dbg ~sr =
		let metadata = Attachments.gsm sr in
		let sr_info = Attachments.get_sr_info sr in
		let device_config = sr_info.Attachments.device_config in
		
		VhdSlave.SR.assert_can_detach ctx metadata device_config;
		let mm = try Some (Attachments.gmm sr) with _ -> None in
		(match mm with | Some m -> VhdMaster.SR.assert_can_detach ctx m device_config | _ -> ());
		(try
			Attachments.log_detachment sr
		with _ -> ());
		(try
			let metadata = Attachments.gsm sr in
			VhdSlave.SR.detach ctx metadata device_config;
			Attachments.detach_as_slave sr;
			Html.signal_slave_metadata_change metadata ();
		with _ -> ());
		(try
			let metadata = Attachments.gmm sr in
			(* This is the point of no return *)
			begin try 
				VhdMaster.SR.detach ctx metadata device_config;
			with e -> 
				log_backtrace ();
				debug "Caught exception: %s. Ignoring" (Printexc.to_string e)
			end;
			Attachments.detach_as_master sr;
			Html.signal_master_metadata_change metadata ()
		with _ -> ());
		Host.remove_pv_id_info sr;
		let driver = Drivers.of_ctx ctx in
		Transport.detach driver sr device_config

	let destroy ctx ~dbg ~sr = 
		(* If we're attached, detach: *)
		let metadata = try Some (Attachments.gsm sr) with _ -> None in
		let sr_info = Attachments.get_sr_info sr in
		let device_config = sr_info.Attachments.device_config in

		(match metadata with
			| Some _ -> detach ctx ~dbg ~sr;
			| None -> ());
		(* Now we can delete *)
		let driver = Drivers.of_ctx ctx in
		let path = Transport.attach driver sr device_config false in
		Transport.delete driver sr device_config path 

	let content_type ctx device_config sr = "phy"

	(* These are internal calls that occur as part of a SR.attach on a
	   slave.  *)
	let slave_attach context tok sr host ids = VhdMaster.SR.slave_attach context (Attachments.gmm sr) tok host ids
	let slave_detach context tok sr host = VhdMaster.SR.slave_detach context (Attachments.gmm sr) tok host

	(* An internal call used as part of the attach process. This is
	   called on a slave when the master has restarted and is
	   reattaching its SRs. *)
	let slave_recover ctx tok sr master =
		let metadata = Attachments.gsm sr in
		if master.h_uuid = Global.get_host_uuid () then begin
		  metadata.Vhd_types.s_rpc <- (!Vhdrpc.local_rpc)
		end else begin 
		  metadata.Vhd_types.s_rpc <- (fun task -> Vhdrpc.remote_rpc task (match master.h_ip with Some x -> x | None -> "unknown") master.h_port)
		end;
		VhdSlave.SR.slave_recover ctx metadata tok master

	(* Part of the infrastructure to support thin provisioning *)
	let thin_provision_check ctx sr = VhdSlave.SR.thin_provision_check ctx (Attachments.gsm sr)

	let list context ~dbg =
	  let m_srs = Attachments.map_master_srs (fun k v -> k) in
	  let s_srs = Attachments.map_master_srs (fun k v -> k) in
	  let all = m_srs @ s_srs in
	  Listext.List.setify all

	let reset context ~dbg ~sr =
	  ()

	let update_snapshot_info_src ctx ~dbg ~sr ~vdi ~url ~dest ~dest_vdi ~snapshot_pairs =
	  failwith "Unimplemented"

	let update_snapshot_info_dest ctx ~dbg ~sr ~vdi ~src_vdi ~snapshot_pairs = 
	  failwith "Unimplemented"

	let stat ctx ~dbg ~sr =
	  { total_space = 0L;
	    free_space = 0L; }
end

module VDI = struct
	let create ctx ~dbg ~sr ~vdi_info =
	  let size = vdi_info.virtual_size in
	  let sm_config = vdi_info.sm_config in
		info "API call: VDI.create sr=%s size=%Ld sm_config=[%s]" sr size (String.concat "; " (List.map (fun (a,b) -> Printf.sprintf "'%s','%s'" a b) sm_config));
		let metadata = Attachments.gmm sr in
		VhdMaster.VDI.create ctx metadata vdi_info

	let add_to_sm_config ctx ~dbg ~sr ~vdi ~key ~value = 
	  info "API call: VDI.add_to_sm_config sr=%s vdi=%s key=%s value=%s" sr vdi key value;
	  let metadata = Attachments.gmm sr in
	  VhdMaster.VDI.add_to_sm_config ctx dbg metadata vdi key value

	let remove_from_sm_config ctx ~dbg ~sr ~vdi ~key =
	  info "API call: VDI.remove_from_sm_config sr=%s vdi=%s key=%s" sr vdi key;
	  let metadata = Attachments.gmm sr in
	  VhdMaster.VDI.remove_from_sm_config ctx dbg metadata vdi key 

	let compose ctx ~dbg ~sr ~vdi1 ~vdi2 = 
	  info "API call: VDI.compose sr=%s vdi1=%s vdi2=%s" sr vdi1 vdi2;
	  let metadata = Attachments.gmm sr in
	  VhdMaster.VDI.compose ctx dbg metadata vdi1 vdi2

	let set_content_id ctx ~dbg ~sr ~vdi ~content_id = 
	  info "API call: VDI.set_content_id sr=%s vdi=%s content_id=%s" sr vdi content_id;
	  let metadata = Attachments.gmm sr in
	  VhdMaster.VDI.set_content_id ctx dbg metadata vdi content_id

	let get_by_name ctx ~dbg ~sr ~name =
	  failwith "Unimplemented"

	let similar_content ctx ~dbg ~sr ~vdi =
	  failwith "Unimplemented"

	let get_url ctx ~dbg ~sr ~vdi =
	  failwith "Unimplemented"

	let epoch_end ctx ~dbg ~sr ~vdi = ()

	let epoch_begin ctx ~dbg ~sr ~vdi = ()

	let set_persistent ctx ~dbg ~sr ~vdi ~persistent =
	  info "API call: VDI.set_persistent sr=%s vdi=%s persistent=%b" sr vdi persistent;
	  let metadata = Attachments.gmm sr in
	  VhdMaster.VDI.set_persistent ctx dbg metadata vdi persistent

	let stat ctx ~dbg ~sr ~vdi =
	  info "API call: VDI.stat";
	  let metadata = Attachments.gmm sr in
	  VhdMaster.VDI.stat ctx dbg metadata vdi

	let update ctx gp sr vdi =
		info "API call: VDI.update";
		let metadata = Attachments.gmm sr in
		VhdMaster.VDI.update ctx metadata gp vdi

	let destroy ctx ~dbg ~sr ~vdi =
		info "API call: VDI.delete sr=%s vdi=%s" sr vdi;
		let metadata = Attachments.gmm sr in
		VhdMaster.VDI.delete ctx metadata vdi

	let snapshot ctx ~dbg ~sr ~vdi_info =
	  info "API call: VDI.snapshot sr=%s vdi=%s" sr vdi_info.vdi ;
	  let metadata = Attachments.gmm sr in
	  VhdMaster.VDI.snapshot ctx metadata vdi_info

	let clone ctx ~dbg ~sr ~vdi_info =
	  info "API call: VDI.clone sr=%s vdi=%s" sr vdi_info.vdi;
	  let metadata = Attachments.gmm sr in
	  VhdMaster.VDI.clone ctx metadata vdi_info

	let resize ctx ~dbg ~sr ~vdi ~new_size =
		info "API call: VDI.resize sr=%s vdi=%s newsize=%Ld" sr vdi new_size;
		let metadata = Attachments.gmm sr in
		VhdMaster.VDI.resize ctx metadata vdi new_size

	let resize_online ctx gp sr vdi newsize =
		info "API call: VDI.resize_online sr=%s vdi=%s newsize=%Ld" sr vdi newsize;
		let metadata = Attachments.gmm sr in
		VhdMaster.VDI.resize ctx metadata vdi newsize

	let attach ctx ~dbg ~dp ~sr ~vdi ~read_write =
		info "API call: VDI.attach sr=%s vdi=%s writable=%b" sr vdi read_write;
		let metadata = Attachments.gsm sr in
		VhdSlave.VDI.attach ctx metadata vdi read_write

	let detach ctx ~dbg ~dp ~sr ~vdi =
		info "API call: VDI.detach sr=%s vdi=%s" sr vdi;
		let metadata = Attachments.gsm sr in
		VhdSlave.VDI.detach ctx metadata vdi

	let activate ctx ~dbg ~dp ~sr ~vdi =
		info "API call: VDI.activate sr=%s vdi=%s" sr vdi;
		let metadata = Attachments.gsm sr in
		VhdSlave.VDI.activate ctx metadata vdi

	let deactivate ctx ~dbg ~dp ~sr ~vdi =
		info "API call: VDI.deactivate sr=%s vdi=%s" sr vdi;
		let metadata = Attachments.gsm sr in
		VhdSlave.VDI.deactivate ctx metadata vdi

	let generate_config ctx gp sr vdi = 
		info "API call: VDI.generate_config sr=%s vdi=%s" sr vdi;
		let metadata = Attachments.gsm sr in
		VhdSlave.VDI.generate_config ctx metadata gp vdi

	let leaf_coalesce ctx gp sr vdi =
		let metadata = Attachments.gmm sr in
		VhdMaster.VDI.leaf_coalesce ctx metadata gp vdi

	let slave_attach ctx host sr_uuid id writable is_reattach =
		let metadata = Attachments.gmm sr_uuid in
		VhdMaster.VDI.slave_attach ctx metadata host id writable is_reattach

	let get_slave_attach_info ctx sr_uuid id =
		let metadata = Attachments.gmm sr_uuid in
		VhdMaster.VDI.get_slave_attach_info ctx metadata id 

	let slave_detach ctx host sr_uuid id =
		let metadata = Attachments.gmm sr_uuid in
		VhdMaster.VDI.slave_detach ctx metadata host id

	let slave_activate ctx host sr_uuid id is_reactivate =
		let metadata = Attachments.gmm sr_uuid in
		VhdMaster.VDI.slave_activate ctx metadata host id is_reactivate

	let slave_deactivate ctx host sr_uuid id =
		let metadata = Attachments.gmm sr_uuid in
		VhdMaster.VDI.slave_deactivate ctx metadata host id

	let slave_reload ctx sr_uuid ids =
		let metadata = Attachments.gsm sr_uuid in
		VhdSlave.VDI.slave_reload ctx metadata ids

	let slave_leaf_coalesce_stop_and_copy ctx sr_uuid id leaf_path new_leaf_path =
		let metadata = Attachments.gsm sr_uuid in
		VhdSlave.VDI.slave_leaf_coalesce_stop_and_copy ctx metadata id leaf_path new_leaf_path

	let external_clone ctx sr_uuid filename =
		let metadata = Attachments.gmm sr_uuid in
		VhdMaster.VDI.external_clone ctx metadata filename

	let slave_set_phys_size ctx sr_uuid id size =
		let metadata = Attachments.gsm sr_uuid in
		VhdSlave.VDI.slave_set_phys_size ctx metadata id size

	let thin_provision_request_more_space ctx sr_uuid host dmnaps =
		debug "sr_uuid=%s" sr_uuid;
		let metadata = Attachments.gmm sr_uuid in
		VhdMaster.VDI.thin_provision_request_more_space ctx metadata host dmnaps

	let attach_from_config ctx gp sr vdi config =
		SR.attach_nomaster ctx gp sr;

		let metadata = Attachments.gsm sr in
		if metadata.Vhd_types.s_data.Vhd_types.s_ready then begin
			let result = VhdSlave.VDI.attach ctx metadata vdi true in (* Attach from config always attached RW *)
			VhdSlave.VDI.activate ctx metadata vdi;
			result
		end else begin
			let slave_attach_info = slave_attach_info_of_rpc (Jsonrpc.of_string config) in
			VhdSlave.VDI.attach_and_activate_from_config ctx metadata gp vdi slave_attach_info
		end


end

module Host = struct
	let set_dead context uuid =
		ignore(Attachments.map_master_srs (fun sr_uuid metadata ->
			VhdMaster.SR.set_host_dead context metadata uuid;
			metadata
		))

	let rolling_upgrade_finished context =
		ignore(Attachments.map_master_srs (fun sr_uuid metadata ->
			VhdMaster.SR.set_rolling_upgrade_finished context metadata))
end

module Debug = struct
	let vdi_get_leaf_path context sr_uuid id =
		let metadata = Attachments.gsm sr_uuid in
		Nmutex.execute context metadata.Vhd_types.s_mutex "Getting leaf path"
			(fun () ->
				let savi = Hashtbl.find metadata.Vhd_types.s_data.Vhd_types.s_attached_vdis id in
				savi.Vhd_types.savi_link)

end
