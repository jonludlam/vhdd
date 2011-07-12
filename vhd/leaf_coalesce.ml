open Threadext
open Vhd_types
open Int_types
open Smapi_types
open Vhd_records

module D=Debug.Debugger(struct let name="leaf_coalesce" end)
open D

exception Cant_leaf_coalesce of string

let clone_and_coalesce context metadata id =
	let vhd_B = Locking.with_clone_lock context metadata id (fun leaf_info ->
		(* Assumption: We have a VHD chain that looks like this:
		   
		   ...->A->B

		   B is the current leaf, A is its parent (maybe raw)
		   We create a new VHD to make the chain:

		   ...->A->B->C

		   This function leaves B the same size as before. ie. it uses more than the virtual_size of the leaf in extra space.
		*)

		let ptr_to_B = leaf_info.leaf in
		let vhd_B = match ptr_to_B with | PVhd vhduid -> Vhd_records.get_vhd context metadata.m_data.m_vhds vhduid | _ -> (raise (Cant_leaf_coalesce "Not a VHD")) in
		let ptr_to_A = match vhd_B.parent with Some p -> p | None -> (raise (Cant_leaf_coalesce "No parent")) in


		let children_of_my_parent = Vhd_records.get_children_from_pointer context metadata.m_data.m_vhds ptr_to_A in

		if List.length children_of_my_parent <> 1 then (raise (Cant_leaf_coalesce "Not an only child"));
		
		let virtual_size = Vhdutil.get_virtual_size vhd_B.size in

		debug "About to create new child";

		(* 'is_attached_rw' is not necessarily correct - attaches and detaches can run concurrently with clone. Doesn't matter though since
		   the reattach later will fix it. *)
		let is_attached_rw = match leaf_info.attachment with | Some (AttachedRW _) -> true | _ -> false in
		let (vhd_C,lvsize_C) = Master_utils.create_child context metadata leaf_info.reservation_override virtual_size vhd_B.location ptr_to_B is_attached_rw in

		debug "Remapping the id to the new child";
		Id_map.remap_id context metadata id (PVhd vhd_C.vhduid);

		debug "Calling reattach";
		Master_utils.reattach context metadata id;

		vhd_B) in
	
	debug "About to resize and move data...";
	let childsize = Coalesce.resize_and_move_data context metadata None None vhd_B.vhduid in
	
	debug "Relinking and reattaching";
	Coalesce.relink_children context metadata vhd_B.vhduid;
	debug "Done"

let reattach_and_stop_and_copy_on_host context metadata id leaf_uid leaf_info host =
	let is_attached_rw = 
		match leaf_info.attachment with
			| None -> false
			| Some (AttachedRW hosts) -> true
			| Some (AttachedRO hosts) -> false
	in

	let sai = Master_utils.get_slave_attach_info_inner context metadata id is_attached_rw leaf_info in
	Master_utils.reattach_nolock context metadata id sai host;

	debug "About to stop-and-copy";
	let ssa = Nmutex.execute context metadata.m_attached_hosts_lock "Finding IP of host"
		(fun () -> List.find (fun ssa -> ssa.ssa_host.h_uuid = host) metadata.m_data.m_attached_hosts) in

	let vhd = Vhd_records.get_vhd context metadata.m_data.m_vhds leaf_uid in
	
	let parent_ptr = match vhd.parent with | Some ptr -> ptr | None -> failwith "No parent!" in
	
	let new_leaf_location = match parent_ptr with
		| PVhd vhduid ->
			(Vhd_records.get_vhd context metadata.m_data.m_vhds vhduid).location
		| PRaw loc ->
			loc
	in

	let container = Locking.with_container_read_lock context metadata (fun () ->
		metadata.m_data.m_vhd_container) in

	let new_leaf_path = Lvmabs.path context container new_leaf_location in
	let leaf_path = Lvmabs.path context container vhd.location in

	let leaf_path =
		if !Global.dummy
		then Stringext.String.replace (Global.get_host_local_dummydir ()) "" leaf_path
		else leaf_path
	in

	let new_leaf_path =
		if !Global.dummy
		then Stringext.String.replace (Global.get_host_local_dummydir ()) "" new_leaf_path
		else new_leaf_path
	in

	Int_client_utils.master_retry_loop context [] [] (fun rpc ->
		Int_client.Vdi.slave_leaf_coalesce_stop_and_copy
			rpc	host metadata.m_data.m_sr_uuid id leaf_path new_leaf_path) metadata ssa.ssa_host;
	
	parent_ptr


let leaf_coalesce context metadata id =
	let rec loop n =
		clone_and_coalesce context metadata id;
		if n>3 then () else loop (n+1)
	in

	loop 0;

	
	let (dead_vhd_uid,new_leaf_ptr,need_reattach) = Locking.with_leaf_coalesce_lock context metadata id (fun leaf_info ->

		let leaf_status = match leaf_info.attachment with
			| None -> None
			| Some (AttachedRW _) -> Some true
			| Some (AttachedRO _) -> Some false
		in
				
		let leaf_uid = match leaf_info.leaf with
			| PVhd vhduid -> vhduid
			| _ -> failwith "Expecting a VHD leaf!"
		in

		Coalesce.resize_parent context metadata leaf_info.reservation_override leaf_status leaf_uid;

		(* Now we have to either attach it and move the data on the master in the case that it's not active anywhere,
		   or we reload the VDI and do stop-and-copy on the slave *)

		let host = Nmutex.execute context metadata.m_id_mapping_lock "Getting host on which VDI is active" (fun () ->
			let leaf_info = Locking.get_leaf_info metadata id in
			match leaf_info.active_on with
				| Some (ActiveRW host) -> Some host
				| _ -> None
		) in

		let parent_ptr = (match host with
			| Some h ->
				reattach_and_stop_and_copy_on_host context metadata id leaf_uid leaf_info h
			| None -> 
				snd (Coalesce.attach_and_move_data context metadata leaf_uid)) in
		
		debug "Done. Fixing up id_to_leaf_map";
		(* Remap the VDI id to point to the parent *)
		Id_map.remap_id context metadata id parent_ptr;

		(* We need to reattach if there wasn't a host to run the stop-and-copy on
		   (which happens if the VDI was attached RO or not active anywhere), or
		   if the VDI was attached on more hosts than just the one it was active
		   on *)
		let need_reattach = host=None ||
			match leaf_info.attachment with 
				| Some (AttachedRW x) -> true
				| Some (AttachedRO x) -> true
				| None -> false
		in
		(leaf_uid,parent_ptr,need_reattach))
	in

	if need_reattach then Master_utils.reattach context metadata id;

		
	debug "Removing/Refreshing VHD metadata";
	Vhd_records.update_hidden context metadata.m_data.m_vhds (PVhd dead_vhd_uid) 2;
	let dead_location = (Vhd_records.remove_vhd context metadata.m_data.m_vhds dead_vhd_uid).location in

	begin
		match new_leaf_ptr with
			| PRaw loc ->
				ignore(Locking.with_container_write_lock context metadata (fun container ->
					(Lvmabs.remove_tag context container loc (Lvm.Tag.of_string "hidden"),())))
			| _ -> ()
	end;
	
	Vhd_records.update_hidden context metadata.m_data.m_vhds new_leaf_ptr 0;

	debug "Removing LVM info";

	(* Remove the VHD from the LVM info *)
	ignore(Locking.with_container_write_lock context metadata (fun container ->
		let container = Lvmabs.remove context container dead_location in
		(Lvmabs.commit context container,())))

let slave_leaf_coalesce_copy context metadata leaf_path =
	Coalesce.move_data context leaf_path

