open Vhd_types
open Int_types
open Threadext
open Vhd_records

exception RWModeLocked of string list (* VHD is attached with an incompatible RW mode on the hosts *)
exception TimedOut
exception VhdIsAttached

module D = Debug.Make(struct let name="locking" end)
open D


(*********************************************** LVM stuff *****************************************************)
let with_container_read_lock context metadata f =
	Pervasiveext.finally
		(fun () ->
			Rwlock.read_lock metadata.m_container_lock;
			Tracelog.append context (Tracelog.LockTaken "m_container_lock") (Some "Read only");
			f ())
		(fun () ->
			Tracelog.append context (Tracelog.LockReleased "m_container_lock") (Some "Read only");
			Rwlock.read_unlock metadata.m_container_lock)

let with_container_write_lock context metadata f =
	Pervasiveext.finally
		(fun () ->
			Rwlock.write_lock metadata.m_container_lock;
			Tracelog.append context (Tracelog.LockTaken "m_container_lock") None;
			let (container,x) = f metadata.m_data.m_vhd_container in
			metadata.m_data.m_vhd_container <- container;
			(container,x))
		(fun () ->
			Tracelog.append context (Tracelog.LockReleased "m_container_lock") None;
			Rwlock.write_unlock metadata.m_container_lock)

let get_container context metadata =
	with_container_read_lock context metadata (fun () -> metadata.m_data.m_vhd_container)



(*********************************************** ID Mapping stuff **********************************************)
(** Call with metadata.m_id_mapping_lock held *)
let get_leaf_info metadata vdi_location =
	try
		Hashtbl.find metadata.m_data.m_id_to_leaf_mapping vdi_location
	with Not_found ->
		raise (Int_rpc.IntError (e_unknown_location, [vdi_location]))

let set_leaf_info metadata vdi_location leaf_info =
	Hashtbl.replace metadata.m_data.m_id_to_leaf_mapping vdi_location leaf_info

let locked_get_leaf_info context metadata vdi_location =
	Nmutex.execute context metadata.m_id_mapping_lock "Locked get leaf info" (fun () -> get_leaf_info metadata vdi_location)

let with_leaf_op context metadata vdi_location op f =
	let leaf_info = locked_get_leaf_info context metadata vdi_location in
	MLock.with_op context leaf_info.current_operations op (fun () ->
		(* The next line is to check that we haven't been deleted *)
		let leaf_info = locked_get_leaf_info context metadata vdi_location in
		f leaf_info)

let rec update_leaf context metadata vdi_location f =
	let result = Nmutex.execute context metadata.m_id_mapping_lock "Update id_map" (fun () ->
		debug "update_leaf: vdi_location=%s" vdi_location;
		
		let new_leaf_info = f (get_leaf_info metadata vdi_location) in
		set_leaf_info metadata vdi_location new_leaf_info;
		new_leaf_info) in
	Html.signal_master_metadata_change metadata ();
	result

let slave_attach_lock context metadata host vdi_location rw protected_fn =
	with_leaf_op context metadata vdi_location OpAttaching (fun leaf_info -> 
		protected_fn leaf_info;
		update_leaf context metadata vdi_location (fun leaf_info ->
			let new_attachment =
				match leaf_info.attachment,rw with
					| None,true -> AttachedRW [host]
					| Some (AttachedRO hosts), true -> raise (RWModeLocked hosts)
					| Some (AttachedRW hosts), true -> AttachedRW (host::hosts)
					| None,false -> AttachedRO [host]
					| Some (AttachedRO hosts), false -> AttachedRO (host::hosts)
					| Some (AttachedRW hosts), false -> raise (RWModeLocked hosts)
			in
			{leaf_info with attachment = Some new_attachment}))


(* For reattach, we ignore the current operations, as this can happen as a result
   of an operation *)
let slave_attach_reattach context metadata host vdi_location rw protected_fn =
	with_leaf_op context metadata vdi_location OpReattaching (fun leaf_info -> 
		protected_fn leaf_info;
		update_leaf context metadata vdi_location (fun leaf_info ->
			let new_attachment =
				match leaf_info.attachment,rw with
					| None,true -> AttachedRW [host]
					| Some (AttachedRO hosts), true -> raise (RWModeLocked hosts)
					| Some (AttachedRW hosts), true ->
						  if List.mem host hosts
						  then AttachedRW hosts
						  else begin
							  if List.length hosts > 1 then raise (RWModeLocked hosts) else AttachedRW (host::hosts)
						  end
					| None, false -> AttachedRO [host]
					| Some (AttachedRO hosts), false ->
						  if List.mem host hosts then AttachedRO hosts else AttachedRO (host::hosts)
					| Some (AttachedRW hosts), false ->
						  raise (RWModeLocked hosts)
			in
			{leaf_info with attachment = Some new_attachment}))

let slave_attach_unlock context metadata host vdi_location protected_fn =
	with_leaf_op context metadata vdi_location OpDetaching (fun leaf_info -> 
		let e = Int_rpc.IntError(e_not_attached,[vdi_location]) in
		let leaf_info = update_leaf context metadata vdi_location (fun leaf_info ->
			let new_attachment =
				match leaf_info.attachment with
					| Some (AttachedRO hosts) ->
						  if not (List.mem host hosts) then begin
							  warn "slave_attach_unlock: Detaching a slave that isn't attached (id=%s host=%s)." vdi_location host;
							  raise e
						  end;
						  let new_hosts = List.filter (fun h -> h <> host) hosts in
						  if List.length new_hosts = 0 then None else Some (AttachedRO new_hosts)
					| Some (AttachedRW hosts) ->
						  if not (List.mem host hosts) then begin
							  warn "slave_attach_unlock: Detaching a slave that isn't attached (id=%s host=%s)." vdi_location host;
							  raise e
						  end;
						  let new_hosts = List.filter (fun h -> h <> host) hosts in
						  if List.length new_hosts = 0 then None else Some (AttachedRW new_hosts)
					| None ->
						  warn "slave_attach_unlock: Detaching a slave that isn't attached (id=%s host=%s)." vdi_location host;
						  raise e
			in
			{leaf_info with attachment = new_attachment}) in
		protected_fn leaf_info)

let slave_activate context metadata host vdi_location is_reactivate =
	with_leaf_op context metadata vdi_location OpActivating (fun leaf_info -> 
		update_leaf context metadata vdi_location (fun leaf_info -> 
			match leaf_info.active_on, leaf_info.attachment with
				| Some (ActiveRO hosts), Some (AttachedRO hosts2) ->
					  (* Double activating is odd. Let's mark that as an
						 error *)
					  if not is_reactivate then assert (not (List.mem host hosts));
					  if not (List.mem host hosts2) then failwith "Not attached";
					  {leaf_info with active_on = Some (ActiveRO (host::(List.filter (fun h -> h <> host) hosts)))}

				| None, Some (AttachedRO hosts2) ->
					  {leaf_info with active_on = Some (ActiveRO [host])}

				| Some (ActiveRW host'), _ ->
					  if is_reactivate then begin
						  if host<>host' then
							  raise (Int_rpc.IntError (e_vdi_active_elsewhere, [vdi_location]))
						  else
							  leaf_info
					  end else
						  raise (Int_rpc.IntError (e_vdi_active_elsewhere, [vdi_location]))

				| None, Some (AttachedRW hosts) ->
					  let israw = match leaf_info.leaf with | PRaw _ -> true | PVhd _ -> false in
					  if not (List.mem host hosts) then 
						  raise (Int_rpc.IntError (e_vdi_not_attached, [vdi_location]));
					  if israw 
					  then {leaf_info with active_on = Some (ActiveRWRaw [host])} 
					  else {leaf_info with active_on = Some (ActiveRW host)}

				| Some (ActiveRWRaw acthosts), Some (AttachedRW atthosts) ->
					  if not (List.mem host atthosts) then 
						  raise (Int_rpc.IntError (e_vdi_not_attached, [vdi_location]));
					  if not is_reactivate then (if (List.mem host acthosts) then failwith "Double activated!?");
					  {leaf_info with active_on = Some (ActiveRWRaw (host::(List.filter (fun h -> h <> host) acthosts)))}

				| _, None ->
					  raise (Int_rpc.IntError (e_vdi_not_attached, [vdi_location]))

				| _ -> 
					  failwith "Unexpected attachment status!"))

let slave_deactivate context metadata host vdi_location =
	with_leaf_op context metadata vdi_location OpDeactivating (fun leaf_info -> 
		let e = Int_rpc.IntError(e_not_activated,[vdi_location]) in
		update_leaf context metadata vdi_location (fun leaf_info ->
			match leaf_info.active_on with
				| Some (ActiveRO hosts) ->
					  if not (List.mem host hosts) then begin
						  raise e
					  end;
					  let new_hosts = List.filter (fun h -> h <> host) hosts in
					  let new_active_on = if List.length new_hosts = 0 then None else Some (ActiveRO new_hosts) in
					  {leaf_info with active_on=new_active_on}
				| Some (ActiveRW h) ->
					  if not (host=h) then begin
						  raise e
					  end;
					  {leaf_info with active_on=None}
				| Some (ActiveRWRaw hosts) ->
					  if not (List.mem host hosts) then begin
						  raise e
					  end;
					  let new_hosts = List.filter (fun h -> h <> host) hosts in
					  let new_active_on = if List.length new_hosts = 0 then None else Some (ActiveRWRaw new_hosts) in
					  {leaf_info with active_on=new_active_on}
				| None ->
					  raise e))

let string_of_op op =
	match op with
		| OpDelete -> "delete"
		| OpClone -> "clone"
		| OpResize -> "resize"
		| OpLeafCoalesceStopAndCopy -> "leafcoalesce"
		| OpAttaching -> "attaching"
		| OpDetaching -> "detaching"

let with_delete_lock context metadata vdi_location f = with_leaf_op context metadata vdi_location OpDelete f
let with_clone_lock context metadata vdi_location f = with_leaf_op context metadata vdi_location OpClone f
let with_resize_lock context metadata vdi_location f = with_leaf_op context metadata vdi_location OpResize f
let with_leaf_coalesce_lock context metadata vdi_location f = with_leaf_op context metadata vdi_location OpLeafCoalesceStopAndCopy f

let check_i_am_attached context metadata vdi_location host =
	debug "check I am attached: vdi_location='%s' host='%s'" vdi_location host;
	Nmutex.execute context metadata.m_id_mapping_lock "Need to check whether I am attached" (fun () ->
		let leaf_info = get_leaf_info metadata vdi_location in
		match leaf_info.attachment with
			| Some (AttachedRW hosts) ->
				List.iter (debug "attached rw to host '%s'") hosts;
				List.exists ((=) host) hosts
			| Some (AttachedRO hosts) ->
				List.iter (debug "attached ro to host '%s'") hosts;
				List.mem host hosts
			| None ->
				debug "Not attached!";
				false)

let is_attached_rw leaf_info =
	match leaf_info.attachment with
		| Some (AttachedRW _) -> true
		| _ -> false

let is_attached leaf_info =
	match leaf_info.attachment with
		| Some _ -> true
		| _ -> false

let is_attached_on_host leaf_info host =
	match leaf_info.attachment with
		| Some (AttachedRW hosts) -> List.mem host hosts
		| Some (AttachedRO hosts) -> List.mem host hosts
		| None -> false

let is_active_rw_on_multiple_hosts leaf_info =
	match leaf_info.active_on with
		| Some (ActiveRWRaw h) -> List.length h>1
		| _ -> false

let get_all_leaf_infos context metadata =
	Nmutex.execute context metadata.m_id_mapping_lock "Getting all the leaf infos" (fun () ->
		Hashtbl.fold (fun k v acc -> (k,v)::acc) metadata.m_data.m_id_to_leaf_mapping [])




