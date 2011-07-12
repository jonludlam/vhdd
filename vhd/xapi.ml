open Vhd_types
open Smapi_types
open Client
open Threadext
open Vhd_records

module D=Debug.Debugger(struct let name="xapi" end)
open D

let xapirpc xml = Xmlrpcclient.do_xml_rpc_unix ~version:"1.0" ~filename:"/var/xapi/xapi" ~path:"/" xml

let create_vdi generic_params id virtual_size phys_size =
	let uuid = Uuid.to_string (Uuid.make_uuid ()) in
	(match generic_params.gp_xapi_params with
		| Some xp ->
			let session = xp.xgp_session_ref in
			let sr_ref = match xp.xgp_sr_ref with Some r -> r | None -> failwith "Expected an SR ref!" in
			let vdi_ref = Client.VDI.db_introduce
				~rpc:xapirpc ~session_id:session ~uuid:uuid ~name_label:uuid ~name_description:"Cloned by backend"
				~sR:sr_ref ~_type:`user ~sharable:false ~read_only:false ~other_config:[] ~location:id
				~xenstore_data:[] ~sm_config:[] in
			Client.VDI.set_managed ~rpc:xapirpc ~session_id:session ~self:vdi_ref ~value:true;
			Client.VDI.set_virtual_size ~rpc:xapirpc ~session_id:session ~self:vdi_ref ~value:virtual_size;
			Client.VDI.set_physical_utilisation ~rpc:xapirpc ~session_id:session ~self:vdi_ref ~value:phys_size
		| None -> ());
	uuid

let forget_vdi generic_params =
	match generic_params.gp_xapi_params with
		| Some p ->
			let session = p.xgp_session_ref in
			begin
				match p.xgp_vdi_ref with
					| Some vdi ->
						Client.VDI.db_forget ~rpc:xapirpc ~session_id:session ~vdi
					| None -> ()
			end
		| None -> ()

let update_sr context generic_params metadata =
	let container = Locking.with_container_read_lock context metadata (fun () -> metadata.m_data.m_vhd_container) in
	let (physical_size,physical_utilisation,free) = Lvmabs.get_sr_sizes context container in
	let ids = Locking.get_all_leaf_infos context metadata in
	let vhds = Vhd_records.get_vhd_hashtbl_copy context metadata.m_data.m_vhds in
	let virtual_allocation = List.fold_left (fun acc (id,leaf_info) ->
		match leaf_info.leaf with
			| PVhd vhdid ->
				debug "Trying to find vhdid=%s in vhds hashtbl" vhdid;
				(try 
					let vhd = Hashtbl.find vhds vhdid in
					let vsize = Vhdutil.get_virtual_size vhd.size in
					Int64.add vsize acc
				with Not_found ->
					debug "Couldn't find it! hashtbl contains:";
					Hashtbl.iter (fun k v -> debug "%s" k) vhds;
					acc)
			| PRaw location ->
				match Lvmabs.size context container location with
					| (_,Some vsize) ->
						Int64.add vsize acc
					| _ -> 
						warn "Unexpected absence of size info";
						acc) 0L ids
	in
	match generic_params.gp_xapi_params with
		| None -> ()
		| Some p ->
			let session_id = p.xgp_session_ref in
			match p.xgp_sr_ref with
				| Some sr_ref ->
					Client.SR.set_virtual_allocation ~rpc:xapirpc ~session_id ~self:sr_ref ~value:virtual_allocation;
					Client.SR.set_physical_size ~rpc:xapirpc ~session_id ~self:sr_ref ~value:physical_size;
					Client.SR.set_physical_utilisation ~rpc:xapirpc ~session_id ~self:sr_ref ~value:physical_utilisation
				| _ -> ()
	
let update_vdi context generic_params metadata id =
	debug "update_vdi";
	let leaf_info = Locking.get_leaf_info metadata id in
	let container = Locking.with_container_read_lock context metadata (fun () -> metadata.m_data.m_vhd_container) in
	let (virtual_size,physical_utilisation) = 
		match leaf_info.leaf with
			| PVhd vhduid ->
				let vhd = Vhd_records.get_vhd context metadata.m_data.m_vhds vhduid in
				let virtual_size = Vhdutil.get_virtual_size vhd.size in
				let (physical_size,_) = Lvmabs.size context container vhd.location in
				(virtual_size,physical_size)
			| PRaw location ->
				let (physical_size,_) = Lvmabs.size context container location in
				(physical_size,physical_size)
	in
	debug "virtual_size=%Ld physical_utilisation=%Ld" virtual_size physical_utilisation;
	match generic_params.gp_xapi_params with
		| None -> ()
		| Some p ->
			let session_id = p.xgp_session_ref in
			match p.xgp_vdi_ref with
				| Some vdi_ref ->
					Client.VDI.set_virtual_size ~rpc:xapirpc ~session_id ~self:vdi_ref ~value:virtual_size;
					Client.VDI.set_physical_utilisation ~rpc:xapirpc ~session_id ~self:vdi_ref ~value:physical_utilisation;
				| _ -> ()
