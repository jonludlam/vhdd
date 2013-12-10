open Vhd_types
open Int_types
open Listext
open Vhd_records

module D=Debug.Make(struct let name="vhdMaster_utils" end)
open D

(** create_child: Create a child VHD given a parent. The parameter is_attached_rw refers to the
	new child - whether or not it is currently attached RW. This is for the case when the leaf
	is being cloned while it's currently attached, or the case of live-leaf coalesce. *)

let create_child context metadata reservation_override virtual_size parent_location parent_ptr is_attached_rw =
	let parent_is_raw = match parent_ptr with | PVhd _ -> false | PRaw _ -> true in

	let reservation_mode =
		match reservation_override with
			| Some m -> reservation_override
			| None -> metadata.m_data.m_lvm_reservation_mode
	in

	(* The LV size is what is determined by the attachment status *)
	let lvsize = Vhdutil.get_lv_size reservation_mode (Some is_attached_rw)
		(Vhdutil.get_size_for_new_vhd virtual_size) in

	debug "Create child: maybe about to create a LV, size=%Ld" lvsize;

	(* The location_uuid is used by nothing else at all. *)
	let location_uuid = Uuidm.to_string (Uuidm.create Uuidm.(`V4)) in

	let (container,location_info) =
		Locking.with_container_write_lock context metadata (fun container ->
			let container,location_info = Lvmabs.create context container location_uuid lvsize in
			let container = Lvmabs.commit context container in
			(container,location_info)) in

	debug "Created LVM volume with uuid=%s" location_uuid;
	Html.signal_master_metadata_change metadata ();

	let (uid,size) =
		Lvmabs.with_active_vhd context container parent_location false (fun ty parent_nod ->
			Lvmabs.with_active_vhd context container location_info false
				(fun ty nod ->
					Vhd.snapshot nod virtual_size parent_nod Vhdutil.max_size (if parent_is_raw then [Vhd.Flag_creat_parent_raw] else []);
					Vhdutil.with_vhd nod false (fun vhd ->
						let uid = Vhd.get_uid vhd in
						let size = Vhdutil.query_size_vhd vhd in
						(uid,size))))
	in

	(* The new vhd_info type *)
	let new_vhd = {
		vhduid=uid;
		parent=Some parent_ptr;
		location=location_info;
		size=size;
		hidden=0;}
	in

	(* Stick the VHD into the VHD tree, and signal the metadata change *)
	Vhd_records.add_vhd context metadata.m_data.m_vhds uid new_vhd;
	
	Html.signal_master_metadata_change metadata ();

	debug "New VHD inserted into Hashtbl uid=%s" uid;

	(new_vhd,lvsize)


(** Resize_vhd: Change the size of the logical volume enclosing a VHD, and rewrite the VHD's footer
	such that it's in the correct place. On file-based SRs, this function does nothing. The is_attached_rw
	parameter is required to determine correctly the size that the LV should be. Returns a boolean
	representing whether the LV was resized or not. *)
let resize_vhd context metadata vhd reservation_override leaf_status =
	debug "Resizing VHD uid: %s" vhd.vhduid;

	let container = Locking.with_container_read_lock context metadata (fun () -> metadata.m_data.m_vhd_container) in
	let size = Vhdutil.update_phys_size vhd.size
		(Lvmabs.with_active_vhd context container vhd.location true (fun ty nod -> Vhdutil.with_vhd nod false (Vhd.get_phys_size))) in

	let reservation_mode =
		match reservation_override with
			| Some m -> reservation_override
			| None -> metadata.m_data.m_lvm_reservation_mode
	in

	let new_size = Vhdutil.get_lv_size reservation_mode leaf_status size in
	match Lvmabs.size context container vhd.location with
		| (_,Some old_size) ->
			Opt.default false (Opt.map (fun _ ->
				if old_size <> new_size then begin
					debug "old_size=%Ld new_size=%Ld" old_size new_size;

					if metadata.m_data.m_rolling_upgrade then begin
						warn "We're in rolling upgrade mode, but I really want to resize a VHD!";
						warn "old_size=%Ld new_size=%Ld LV=%s" old_size new_size (Lvmabs.string_of_location_info vhd.location);
						false;
					end else begin 
						let (container,_) = Locking.with_container_write_lock context metadata (fun container ->
							(Lvmabs.resize context container vhd.location new_size,()))
						in
						
						Lvmabs.with_active_vhd context container vhd.location false (fun ty nod ->
							Vhdutil.wipe_footer nod;
							Vhdutil.with_vhd nod true (fun vhd ->
								Vhd.set_phys_size vhd new_size);
						);
						true
					end
				end else begin
					debug "new_size=old_size=%Ld. Not doing anything" new_size;
					false
				end) metadata.m_data.m_lvm_reservation_mode)
		| (_,None) -> false



let get_slave_attach_info_inner context metadata id writable leaf_info =
	let container = Locking.get_container context metadata in
	let (path,maxsize,phys_size,is_raw,vhds,rawlvopt) =
		match leaf_info.leaf with
			| PVhd vhduid ->
				let (vhds,rawlvopt) = Vhd_records.get_vhd_chain context metadata.m_data.m_vhds vhduid in
				let vhdrec = List.hd vhds in
				let path = Lvmabs.path context container vhdrec.location in
				let (_,maxsize) = Lvmabs.size context container vhdrec.location in
				let phys_size = Vhdutil.get_phys_size vhdrec.size in
				(path,maxsize,phys_size,false,vhds,rawlvopt)
			| PRaw loc ->
				let path = Lvmabs.path context container loc in
				let (_,maxsize) = Lvmabs.size context container loc in
				match maxsize with 
					| Some phys_size ->
						(path,maxsize,phys_size,true,[],Some loc)
					| _ -> 
						error "Didn't get a size for a raw location!";
						failwith "Unexpected error!"
	in
	let vhd_lvs = List.filter_map (fun vhd -> Lvmabs.get_attach_info context container vhd.location) vhds in
	let all_lvs = match rawlvopt with
		| None -> vhd_lvs
		| Some loc -> (match (Lvmabs.get_attach_info context container loc) with 
			| Some stuff -> stuff::vhd_lvs
			| None -> vhd_lvs)
	in
	let path = 
		if !Global.dummy
		then Stringext.String.replace (Global.get_host_local_dummydir ()) "" path
		else path
	in
	{sa_leaf_path=path;
	sa_leaf_maxsize=maxsize;
	sa_leaf_phys_size=phys_size;
	sa_leaf_is_raw=is_raw;
	sa_writable=writable;
	sa_lvs=all_lvs}


(** Reattach: This function is a helper that calls out to all slaves on which the VDI is attached,
	causing them to reload the metadata associated with this particular VDI. This will be required usually
	because the VHD chain has been changed in some way (snapshot/coalesce/etc.). It takes a snapshot of
	the places on which the VDI is attached and calls out to those hosts. Any new attachments will already be using
	the new metadata, so that's OK. *)

let reattach_nolock context metadata id sai host =
	debug "Reattaching host: %s" host;
	try
		let ssa = Nmutex.execute context metadata.m_attached_hosts_lock "Find the host's IP" 
			(fun () -> List.find (fun ssa -> ssa.ssa_host.h_uuid = host) metadata.m_data.m_attached_hosts) in
		Int_client_utils.master_retry_loop context [e_not_attached; e_sr_not_attached] [] (fun client ->
		  let module Client = (val client : Int_client.CLIENT) in
		  Client.VDI.slave_reload ~sr:metadata.m_data.m_sr_uuid ~vdis:[(id,sai)])
		  metadata ssa.ssa_host
	with Not_found ->
		debug "Host not found - assuming it's been shut down"

let reattach context metadata id =
	debug "Calling reattach";

	let (hosts,sai) = 
		Locking.with_leaf_op context metadata id OpReattaching (fun leaf_info -> 

			let hosts,is_attached_rw = 
				match leaf_info.attachment with
					| None -> [], false
					| Some (AttachedRW hosts) -> hosts, true
					| Some (AttachedRO hosts) -> hosts, false
			in

			begin 
				match leaf_info.leaf with
					| PVhd vhduid ->
						  let vhd = Vhd_records.get_vhd context metadata.m_data.m_vhds vhduid in
						  ignore(resize_vhd context metadata vhd 
							  leaf_info.reservation_override (Some is_attached_rw))
					| PRaw _ -> 
						  ()
			end;
			
			let sai = get_slave_attach_info_inner context metadata id is_attached_rw leaf_info in
		    
			hosts,sai
		) 
	in

	(* For each host, call slave_reload *)
	List.iter (reattach_nolock context metadata id sai) hosts

(** Returns a tuple of hidden value and new size *)
let set_hidden context metadata vhd =
	debug "Calling set_hidden on VHD uid=%s" vhd.vhduid;
	let container = Locking.with_container_read_lock context metadata (fun () -> metadata.m_data.m_vhd_container) in
	Lvmabs.with_active_vhd context container vhd.location false (fun ty nod ->
		Vhdutil.with_vhd nod true (fun vhd ->
			(Vhdutil.set_hidden vhd,Vhdutil.query_size_vhd vhd)))

