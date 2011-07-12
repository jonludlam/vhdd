(******************************* ID MAP ****************************)

open Vhd_types
open Vhd_records

type id_map = {
	id : string;
	lp : Vhd_records.pointer;
	r : Vhdutil.reservation_mode option;
}

and id_map_l = id_map list with rpc

exception NoIdMapStore

let get_id_map_store metadata =
	match metadata.m_data.m_id_mapping_persistent_store with
		| (_,None) -> raise NoIdMapStore
		| (container,Some location) -> (container,location)

let commit_map_to_disk_init context container location mapping =
	let id_map_l = Hashtbl.fold (fun k v acc -> {id=k; lp=v.leaf; r=v.reservation_override}::acc) mapping [] in
	let str = Jsonrpc.to_string (rpc_of_id_map_l id_map_l) in
	Lvmabs.write context container location str

let commit_map_to_disk context metadata =
	let (container,location) = get_id_map_store metadata in
	let mapping = metadata.m_data.m_id_to_leaf_mapping in
	commit_map_to_disk_init context container location mapping

let read_map_from_disk context container location =
	let id_map = id_map_l_of_rpc (Jsonrpc.of_string (Lvmabs.read context container location)) in
	let hashtbl = Hashtbl.create (List.length id_map) in
	List.iter (fun r -> Hashtbl.replace hashtbl r.id {current_operations=MLock.create r.id r.id; attachment=None; active_on=None; reservation_override=r.r; leaf=r.lp}) id_map;
	hashtbl

let add_to_id_map context metadata id leaf reservation_override =
	ignore(get_id_map_store metadata);
	let reason = Printf.sprintf "Adding a new ID to the mapping (%s->%s)" id
		(match leaf with
			| PRaw l -> Printf.sprintf "PRaw '%s'" (Lvmabs.string_of_location_info l)
			| PVhd id -> Printf.sprintf "PVhd '%s'" id) in
	Nmutex.execute context metadata.m_id_mapping_lock reason (fun () ->
		let leaf_info =	{
			leaf=leaf; 
			current_operations=MLock.create id id; 
			attachment=None; 
			active_on=None;
			reservation_override=reservation_override}
		in
		Hashtbl.replace metadata.m_data.m_id_to_leaf_mapping id leaf_info;
		commit_map_to_disk context metadata;
		Tracelog.append context (Tracelog.Master_id_to_leaf_add (id,leaf_info)) None;
	);
	Html.signal_master_metadata_change metadata ()

let remove_id_from_map context metadata id =
	let reason = Printf.sprintf "Removing an ID from the mapping (%s)" id in
	Nmutex.execute context metadata.m_id_mapping_lock reason (fun () ->
		Hashtbl.remove metadata.m_data.m_id_to_leaf_mapping id;
		(* If there's no room to create an id_to_leaf_map LVM node, let's at least
		   allow people to destroy disks to make room *)
		(try commit_map_to_disk context metadata with NoIdMapStore -> ());
		Tracelog.append context (Tracelog.Master_id_to_leaf_remove id) None;
	);
	Html.signal_master_metadata_change metadata ()

let remap_id context metadata id new_leaf =
	ignore(get_id_map_store metadata);
	let new_ptr = match new_leaf with
		| PRaw l -> Printf.sprintf "PRaw '%s'" (Lvmabs.string_of_location_info l)
		| PVhd id -> Printf.sprintf "PVhd '%s'" id in
	let reason = Printf.sprintf "Remapping an ID (%s->%s)" id new_ptr in
	Nmutex.execute context metadata.m_id_mapping_lock reason (fun () ->
		let old_leaf_info = Hashtbl.find metadata.m_data.m_id_to_leaf_mapping id in
		let new_leaf_info = {old_leaf_info with leaf=new_leaf} in
		Hashtbl.replace metadata.m_data.m_id_to_leaf_mapping id new_leaf_info;
		Tracelog.append context (Tracelog.Master_id_to_leaf_update (id,new_leaf_info))
			(Some (Printf.sprintf "Remapped (%s->%s)" id new_ptr));
		commit_map_to_disk context metadata;
	);
	Html.signal_master_metadata_change metadata ()

let id_exists context metadata id =
	let reason = Printf.sprintf "Checking to see if an ID exists (%s)" id in
	Nmutex.execute context metadata.m_id_mapping_lock reason (fun () ->
		try ignore(Hashtbl.find metadata.m_data.m_id_to_leaf_mapping id); true with _ -> false)

let get_currently_attached context metadata id =
	let reason = Printf.sprintf "Getting currently attached list (%s)" id in
	Nmutex.execute context metadata.m_id_mapping_lock reason (fun () ->
		let leaf_info = Hashtbl.find metadata.m_data.m_id_to_leaf_mapping id in
		match leaf_info.attachment with
			| None -> []
			| Some (AttachedRO list)
			| Some (AttachedRW list) -> list)


let initialise_id_map context container location_option vhds do_init =
	if not do_init then 
		match location_option with 
			| Some location -> 
				read_map_from_disk context container location
			| None -> failwith "Odd error - expecting a container and location in initialise_id_map"
	else
		(* This should only be used in the case of upgrade from python SM backends *)
		let hashtbl = Hashtbl.create (List.length vhds) in
		List.iter (fun vhd ->
			let id = vhd.vhduid in
			if vhd.hidden=0 then begin
				let old_style_id = Lvmabs.location_uuid context vhd.location in
				let size = Vhdutil.update_phys_size vhd.size
					(Lvmabs.with_active_vhd context container vhd.location true (fun ty nod -> Vhdutil.with_vhd nod false (Vhd.get_phys_size)))
				in
				let expected_size = Vhdutil.get_lv_size (Some Vhdutil.Leaf) (Some false) size in
				let reservation_override =
					match Lvmabs.size context container vhd.location with
						| (_,None) -> None
						| (_,Some s) ->
							if s < expected_size then Some Vhdutil.Attach else None
				in
				Hashtbl.replace hashtbl old_style_id {current_operations=MLock.create id id; attachment=None; leaf=PVhd id; active_on=None; reservation_override=reservation_override} 
			end) vhds;

		let hidden_lvs = Lvmabs.get_hidden_lvs context container in
		ignore(Lvmabs.scan context container (fun loc ->
			match (loc.Lvmabs_types.location, loc.Lvmabs_types.location_type) with
				| (Lvmabs_types.LogicalVolume lv_name, Lvmabs_types.Raw) ->
					let old_style_id = Lvmabs.location_uuid context loc in
					if not (List.mem loc hidden_lvs) then 
						Hashtbl.replace hashtbl old_style_id {current_operations=MLock.create old_style_id old_style_id; attachment=None; leaf=PRaw loc; active_on=None; reservation_override=None};
					None
				| _ ->
					None));
		(* If we can, commit this map to disk *)
		(match location_option with 
 			| Some location -> 
				commit_map_to_disk_init context container location hashtbl;
			| None -> ());
		hashtbl

