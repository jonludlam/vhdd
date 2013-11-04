(* Coalesce functionality *)

(* Why is everything done with side-effects here? *)

open Threadext
open Vhd_types
open Int_types
open Listext
open Vhd_records

module D=Debug.Make(struct let name="coalesce" end)
open D



let findDefault default h k = try Hashtbl.find h k with Not_found -> default;;

let check_nod_for_parent nod =
	debug "Checking that %s has a parent" nod;
	let vhd = Vhd._open nod [Vhd.Open_rdonly] in
	try
		Pervasiveext.finally
			(fun () ->
				let p = Vhd.get_parent vhd in
				debug "Parent=%s" p)
			(fun () ->
				Vhd.close vhd;)
	with e ->
		debug "Exception caught and ignored: %s" (Printexc.to_string e)


let syslog s =
	Syslog.log Syslog.Daemon Syslog.Alert (Printf.sprintf "[%d] %s" (Thread.id (Thread.self ())) s)

let with_coalesce_lock context metadata f =
	Nmutex.execute context metadata.m_coalesce_in_progress_lock "setting coalesce_in_progress flag" (fun () ->
		while metadata.m_data.m_coalesce_in_progress do
			Nmutex.condition_wait context metadata.m_coalesce_condition metadata.m_coalesce_in_progress_lock
		done;
		metadata.m_data.m_coalesce_in_progress <- true;
		Tracelog.append context (Tracelog.Master_m_coalesce_in_progress true) None
);
	Html.signal_master_metadata_change metadata ();
	Pervasiveext.finally
		(fun () -> f ())
		(fun () ->
			Nmutex.execute context metadata.m_coalesce_in_progress_lock "Unsetting coalesce_in_progress flag"
				(fun () -> metadata.m_data.m_coalesce_in_progress <- false;
					Tracelog.append context (Tracelog.Master_m_coalesce_in_progress false) None;
					Nmutex.condition_broadcast context metadata.m_coalesce_condition;
					Html.signal_master_metadata_change metadata ()))

let get_new_parent_size context metadata reservation_override leaf_status parentsize childsize =
	let reservation_mode = match reservation_override with
		| Some _ -> reservation_override
		| None -> metadata.m_data.m_lvm_reservation_mode
	in
	let combined_size = Vhdutil.sum_sizes parentsize childsize in
	Vhdutil.get_lv_size	reservation_mode leaf_status combined_size

let move_data context childnod =
	syslog "About to use libvhd to coalesce";
	Vhdutil.with_vhd childnod true (fun vhd ->
		try
			Vhd.coalesce vhd;
			Vhd.set_hidden vhd 2; (* Mark the data transfer as having happened *)
		with e ->
			log_backtrace ();
			debug "Caught exception!!: %s" (Printexc.to_string e);
			Thread.delay 600.0);
	syslog "done";
	0

(* FIXME: libvhd seems to require all VHD ancestors to be active to do a coalesce *)
let attach_and_move_data context metadata vhduid =
	let (vhds,rawopt) = Vhd_records.get_vhd_chain context metadata.m_data.m_vhds vhduid in
	let vhd = List.hd vhds in
	let parent_ptr = match vhd.Vhd_records.parent with | Some ptr -> ptr | None -> failwith "Cant coalesce" in
	let vhd_locs = List.map (fun vhd -> vhd.location) (List.tl vhds) in
	let locations = match rawopt with | Some loc -> loc::vhd_locs | None -> vhd_locs in

	let container = Locking.with_container_read_lock context metadata (fun () ->
		metadata.m_data.m_vhd_container) in
	
	let inner () =
		Lvmabs.with_active_vhd context container vhd.location false (fun _ childnod ->
			let size = move_data context childnod in
			Vhd_records.update_hidden context metadata.m_data.m_vhds (PVhd vhduid) 2;
			size)
	in

	let rec attachem locs =
		match locs with
			| [] -> inner ()
			| loc::locs -> 
				Lvmabs.with_active_vhd context container loc false (fun _ _ -> 
					attachem locs)
	in 

	let size = attachem locations in

	(size,parent_ptr)



		
let resize_parent context metadata reservation_override leaf_status vhduid =
	try
		debug "Beginning coalesce of VHD %s" vhduid;
		
		let action = Printf.sprintf "Getting records for VHDs: %s and its parent" vhduid in
		let vhd = Vhd_records.get_vhd context metadata.m_data.m_vhds vhduid in
		
		let container = Locking.with_container_read_lock context metadata (fun () ->
			metadata.m_data.m_vhd_container) in
		
		Lvmabs.with_active_vhd context container vhd.location false (fun _ childnod ->
			let size  = Vhdutil.query_size childnod in
			
			let (container,new_parent_size_opt,parent_location) =
				match vhd.parent with
					| Some (PVhd parentuid) -> begin
						(* Here we might need to resize the parent VHD *)
						
						let parentvhd = Vhd_records.get_vhd context metadata.m_data.m_vhds parentuid in
						
						debug "VHD location: %s" (Lvmabs.string_of_location_info vhd.location);
						debug "Parent uid: %s location: %s " parentuid (Lvmabs.string_of_location_info parentvhd.location);
						
						let parent_size = Lvmabs.with_active_vhd context container parentvhd.location false
							(fun _ parentnod -> Vhdutil.query_size parentnod)
						in
						
						debug "size of child: %s" (Vhdutil.string_of_size size);
						debug "size of parent: %s" (Vhdutil.string_of_size parent_size);
						
						(* Got the sizes of the child and parent - now resize the parent *)
						(* Todo: better estimate of the parent size *)
						let new_parent_size = get_new_parent_size context metadata reservation_override leaf_status parent_size size in
						
						let (container,_) = Locking.with_container_write_lock context metadata (fun container ->
							let container = Lvmabs.resize context container parentvhd.location new_parent_size in
							debug "Resized parent to be: %Ld" new_parent_size;
							(container,()))
						in
						(container,Some new_parent_size,parentvhd.location)
					end
					| Some (PRaw x) ->
						(container,None,x)
					| None -> failwith "No parent to coalesce into!"
			in
			
			Lvmabs.with_active_vhd context container parent_location false (fun ty parentnod ->
				begin
					match new_parent_size_opt with
						| Some new_parent_size ->
							if Lvmabs.is_lvm context container then begin
								debug "Setting physical size of parent to be: %Ld" new_parent_size;
								
								Vhdutil.wipe_footer parentnod;
								(*check_nod_for_parent parentnod;*)
								
								syslog "About to use libvhd to rewrite the footer in coalesce";
								
								Vhdutil.with_vhd parentnod true (fun parent ->
									Vhd.set_phys_size parent (Vhdutil.get_footer_pos_from_phys_size new_parent_size));
								syslog "Done";
							end;
						| None -> ();
				end
			)
		)
	with e ->
		log_backtrace ();
		debug "Caught exception while coalescing: %s" (Printexc.to_string e);
		raise e

let resize_and_move_data context metadata reservation_override leaf_status vhduid =
	resize_parent context metadata reservation_override leaf_status vhduid;
	attach_and_move_data context metadata vhduid

let remove_unreachable_vhds context metadata = ()


let mark_as_hidden context metadata ptr = 
	warn "Warning: Found a parent that wasn't marked as hidden: %s. Fixing."
		(match ptr with
			| PVhd x -> Printf.sprintf "(PVhd '%s')" x
			| PRaw x -> Printf.sprintf "(PRaw '%s')" (Lvmabs.string_of_location_info x));
	let hidden = match ptr with
		| PVhd x -> 
			let vhd = Vhd_records.get_vhd context metadata.m_data.m_vhds x in
			let (hidden,new_size) = Master_utils.set_hidden context metadata vhd in
			Vhd_records.update_vhd_size context metadata.m_data.m_vhds x new_size;
			hidden
		| PRaw x -> 
			ignore(Locking.with_container_write_lock context metadata
				(fun container ->
					(Lvmabs.add_tag context container x (Lvm.Tag.of_string "hidden"),())));
			1
	in
	Vhd_records.update_hidden context metadata.m_data.m_vhds ptr hidden


(** Type representing the classification of the contents of an SR *)
type classification_t = {
	coalescable_vhds : string list;
	leaf_coalescable_ids : string list;
	unreachable_vhds : (string * bool) list;
	unreachable_lvs : (Lvmabs_types.location_info * bool) list;
	broken_vhds : string list;
	bad_parents : Vhd_records.pointer list;
}

let classify_sr_contents context metadata =
	let (coalescable_not_hidden,coalescable_hidden,relinkable,unreachable) = Vhd_records.get_coalesce_info context metadata.m_data.m_vhds in
	let id_map = Locking.get_all_leaf_infos context metadata in
	let vhd_leaf_to_id_map = List.filter_map (fun (id,leaf_info) -> match leaf_info.leaf with | PVhd vhduid -> Some (vhduid, id) | PRaw _ -> None) id_map in 
	let leaf_coalescable_ids = List.filter_map (fun vhduid -> try Some (List.assoc vhduid vhd_leaf_to_id_map) with Not_found -> None) coalescable_not_hidden in
	let unreachable_vhds = List.filter_map (fun ptr -> match ptr with | PVhd vhduid -> Some vhduid | _ -> None) unreachable in
	let unreachable_lvs = List.filter_map (fun ptr -> match ptr with | PRaw loc -> Some loc | _ -> None) unreachable in
	debug "Coalescable_vhds: [%s]" (String.concat ";" coalescable_hidden);
	debug "Leaf_Coalescable_ids: [%s]" (String.concat ";" leaf_coalescable_ids);
	debug "Unreachable_vhds: [%s]" (String.concat ";" unreachable_vhds);
	debug "Unreachable_lvs: [%s]" (String.concat ";" (List.map Lvmabs.string_of_location_info unreachable_lvs));
	{ 
		coalescable_vhds = coalescable_hidden;
		leaf_coalescable_ids = leaf_coalescable_ids;
		unreachable_vhds = List.filter_map (fun ptr -> match ptr with | PVhd vhduid -> Some (vhduid,true) | _ -> None) unreachable;
		unreachable_lvs = List.filter_map (fun ptr -> match ptr with | PRaw loc -> Some (loc,true) | _ -> None) unreachable;
		broken_vhds = [];
		bad_parents = [];
	}		
		
exception No_parent

let relink_children context metadata vhduid =
	try
		debug "Beginning relink phase of vhd: %s" vhduid;

		let vhds = Vhd_records.get_children_from_pointer context metadata.m_data.m_vhds (PVhd vhduid) in
		let vhd = Vhd_records.get_vhd context metadata.m_data.m_vhds vhduid in
		let parent_location,parent_ptr = 
			match vhd.parent with
				| Some (PVhd p) -> (Vhd_records.get_vhd context metadata.m_data.m_vhds p).location, PVhd p
				| Some (PRaw l) -> l, PRaw l
				| None -> raise No_parent
		in

		let container = Locking.with_container_read_lock context metadata (fun () -> metadata.m_data.m_vhd_container) in

		List.iter (fun (k,v) ->
			let action = Printf.sprintf "About to relink vhd %s" k in
			Lvmabs.with_active_vhd context container parent_location false (fun ty parentnod ->
				Lvmabs.with_active_vhd context container v.location false (fun _ nod ->
					syslog "About to set parent in coalesce";
					let vhd = Vhd._open nod [Vhd.Open_rdwr] in
					Vhd.set_parent vhd parentnod (ty=Lvmabs_types.Raw);
					Vhd.close vhd));
			ignore(Vhd_records.update_vhd_parent context metadata.m_data.m_vhds k vhd.parent);
		) vhds;

		Vhd_records.remove_vhd context metadata.m_data.m_vhds vhduid;

		let all_relinked_vhds = vhds in

		(* Now we have a list of all of the VHDs that have been relinked. Now we need
		   to make sure we request that the tapdisks reopen. Get this by getting a
		   snapshot of the currently-attached tapdisks (note that any subsequent
		   attaches will use the new chain, so we don't care about them) *)

		let all_attached_ids = Locking.get_all_leaf_infos context metadata in

		(* Use this list to find the ids of tapdisks that need refreshing *)

		let affected_vhds = Vhd_records.get_all_affected_vhds context metadata.m_data.m_vhds parent_ptr in

		let ids_to_refresh = List.filter_map (fun (id,leaf_info) -> 
			if Locking.is_attached leaf_info then 
				match leaf_info.leaf with 
					| PVhd vhduid -> if List.mem vhduid affected_vhds then Some id else None
					| _ -> None
			else None) all_attached_ids 
		in

		List.iter (fun id -> debug "id: %s (referencing this VHD) is attached." id) ids_to_refresh;

		List.iter (fun id -> Master_utils.reattach context metadata id) ids_to_refresh;

		(* Everyone has reloaded now: adjust the logical volumes if
		   necessary - remove the LV for the vhd being coalesced, and
		   resize the parent to be the correct size. First need to find
		   the correct size! *)

		if parent_location.Lvmabs_types.location_type=Lvmabs_types.Vhd then begin
			let parent_size = Lvmabs.with_active_vhd context container parent_location false (fun _ parentnod ->
				syslog "About to use libvhd to rewrite the footer in coalesce - 2";
				let vhd' = Vhd._open parentnod [Vhd.Open_rdwr] in
				let parent_size = Vhd.get_phys_size vhd' in
				Vhd.set_phys_size vhd' (Vhdutil.get_footer_pos_from_phys_size parent_size);
				Vhd.close vhd';
				syslog "Done";
				parent_size)
			in

			let (_,()) = Locking.with_container_write_lock context metadata (fun container ->
				let location = vhd.location in
				let container = Lvmabs.remove context container location in
				let container = Lvmabs.resize context container parent_location parent_size in
				(container,())
			) in
			(* Get everyone to reload again *)
			List.iter (fun id -> Master_utils.reattach context metadata id) ids_to_refresh
		end else begin
			(* If the parent was raw, don't need to resize - just remove the VHD LV *)
			let (_,()) = Locking.with_container_write_lock context metadata (fun container ->
				let location = vhd.location in
				let container = Lvmabs.remove context container location in
				container,())
			in
			(* No need to reload *)
			()
		end
	with No_parent -> ()
		
