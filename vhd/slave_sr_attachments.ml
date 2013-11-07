(************************************* Slave SR Attachments ***********************************)
open Vhd_types
open Int_types

module D=Debug.Make(struct let name="slave_sr_attachments" end)
open D

(* Store the attached hosts with resync_required=true (if we need to read this, resync will be required!) *)
let commit_attached_hosts_to_disk context metadata master =
	let attached = List.map (fun ssa -> {ssa with ssa_resync_required = true}) metadata.m_data.m_attached_hosts in
	let str = Jsonrpc.to_string (Vhd_types.rpc_of_slave_sr_attachment_info {l=attached; master=master}) in
	let container,location = metadata.m_data.m_attached_hosts_persistent_store in
	Lvmabs.write context container location str;
	Html.signal_master_metadata_change metadata ()

let read_slave_sr_attachment_info_from_disk context container location =
	try
		let s = Lvmabs.read context container location in
		Vhd_types.slave_sr_attachment_info_of_rpc (Jsonrpc.of_string s)
	with Circ.No_data ->
		{ l=[];
		master=None }

let read_attached_hosts_from_disk context container location =
	(read_slave_sr_attachment_info_from_disk context container location).l

let slave_sr_attach context metadata host ids =
	debug "Slave SR attach";
	List.iter (fun (id,active) -> debug "id=%s active=%b" id active) ids;

	Nmutex.execute context metadata.m_attached_hosts_lock "Adding host to the attached_hosts" (fun () ->
		let hs = List.filter (fun ssa -> ssa.ssa_host <> host) metadata.m_data.m_attached_hosts in
		metadata.m_data.m_attached_hosts <- {ssa_host=host; ssa_resync_required=false}::hs;
		commit_attached_hosts_to_disk context metadata (Some (Global.get_localhost ()));
		Tracelog.append context (Tracelog.Master_m_attached_hosts metadata.m_data.m_attached_hosts)
			(Some (Printf.sprintf "Host '%s' attached" host.h_uuid));
	);

	(* Here we resync our knowledge of what the slave has attached *)
	Nmutex.execute context metadata.m_id_mapping_lock "Resyncing VDI attachments/activations" (fun () ->
		let kvs = Hashtbl.fold (fun k v acc -> (k,v)::acc) metadata.m_data.m_id_to_leaf_mapping [] in

		(* kvs is the master's view of the world *)

		let remove_from_attachment attachment =
			match attachment with
				| Some (AttachedRO l) ->
					let new_l = List.filter (fun x -> x <> host.h_uuid) l in
					if List.length new_l > 0 then Some (AttachedRO new_l) else None
				| Some (AttachedRW l) ->
					let new_l = List.filter (fun x -> x <> host.h_uuid) l in
					if List.length new_l > 0 then Some (AttachedRW new_l) else None
				| None -> None
		in

		let remove_from_active_on active_on =
			match active_on with
				| Some (ActiveRO l) ->
					let new_l = List.filter (fun x -> x <> host.h_uuid) l in
					if List.length new_l > 0 then Some (ActiveRO new_l) else None
				| Some (ActiveRW h) -> if h=host.h_uuid then None else active_on
				| Some (ActiveRWRaw l) -> 
					let new_l = List.filter (fun x -> x <> host.h_uuid) l in
					if List.length new_l > 0 then Some (ActiveRWRaw new_l) else None
				| None -> None
		in

		List.iter (fun (k,v) ->
			let slave_state = try Some (List.assoc k ids) with _ -> None in

			let new_v =
				match slave_state with
					| None -> (* Slave not attached, not activated *)
						let new_attachment = remove_from_attachment v.attachment in
						let new_active_on = remove_from_active_on v.active_on in
						{v with attachment=new_attachment; active_on = new_active_on}

					| Some false -> (* slave attached, not activated *)
						let new_active_on = remove_from_active_on v.active_on in
						{v with active_on = new_active_on}

					| Some true -> (* slave attached, activated *)
						v
			in

			Hashtbl.replace metadata.m_data.m_id_to_leaf_mapping k new_v) kvs;
		
		Tracelog.append context (Tracelog.Master_m_id_to_leaf_mapping (Hashtbl.copy metadata.m_data.m_id_to_leaf_mapping)) (Some "Slave synchronised");
	);

	"OK"

let slave_sr_detach_internal context metadata host_uuid =
	metadata.m_data.m_attached_hosts <- List.filter (fun ssa -> ssa.ssa_host.h_uuid <> host_uuid) metadata.m_data.m_attached_hosts;
	Tracelog.append context (Tracelog.Master_m_attached_hosts metadata.m_data.m_attached_hosts) None

let slave_sr_detach_by_host_uuid context metadata host_uuid =
	Nmutex.execute context metadata.m_attached_hosts_lock "Removing a host from the attachments (by uuid)" (fun () -> 
		slave_sr_detach_internal context metadata host_uuid;
		Nmutex.condition_broadcast context metadata.m_attached_hosts_condition
	)

let slave_sr_detach context metadata host =
	Nmutex.execute context metadata.m_attached_hosts_lock "Removing a host from the attachments" (fun () ->
		if List.exists (fun ssa -> ssa.ssa_host=host) metadata.m_data.m_attached_hosts
		then begin
			slave_sr_detach_by_host_uuid context metadata host.h_uuid;
			Nmutex.condition_broadcast context metadata.m_attached_hosts_condition;
		end else begin
			warn "Locking.slave_sr_detach called, but the host is not attached! (sr_uuid=%s host_uuid=%s host_ip=%s)" metadata.m_data.m_sr_uuid host.h_uuid (match host.h_ip with Some x -> x | None -> "unknown");
		end)

let get_attached_hosts context metadata =
	Nmutex.execute context metadata.m_attached_hosts_lock "Retrieving the hashtbl of attached hosts" (fun () -> metadata.m_data.m_attached_hosts)
