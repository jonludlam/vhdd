open Vhd_types
open Threadext
open Int_types
open Master_utils
open Vhd_records

module D=Debug.Make(struct let name="clone" end)
open D

let syslog s =
	Syslog.log Syslog.Daemon Syslog.Alert (Printf.sprintf "[%d] %s" (Thread.id (Thread.self ())) s)



let clone context metadata generic_params vdi new_reservation_override =
	let id = vdi in
	Locking.with_clone_lock context metadata id
		(fun leaf_info ->
			(* Note - we disallow attach/detach/activate/deactivate while an operation is
			   in progress. Therefore the following boolean is true for the duration of
			   this function *)
			let is_attached_rw = Locking.is_attached_rw leaf_info in


			if Locking.is_active_rw_on_multiple_hosts leaf_info then
				failwith "RAW VDI is active on multiple hosts: Can't clone";

			let parent_ptr = leaf_info.leaf in

			let (virtual_size,parent_location) =
				match parent_ptr with
					| PVhd x -> begin
						let parent_vhd = Vhd_records.get_vhd context metadata.m_data.m_vhds x in
						(Vhdutil.get_virtual_size parent_vhd.size, parent_vhd.location)
					end
					| PRaw location -> begin
						let container = Locking.with_container_read_lock context metadata (fun () -> metadata.m_data.m_vhd_container) in
						let virtual_size =
							match (Lvmabs.size context container location) with | (_,Some x) -> x | (_,None) -> failwith "Unable to determine parent size"
						in
						(virtual_size,location)
					end
			in

			debug "Leaf clone: creating new leaf vhd for original VDI";
			let (vhd1,lvsize1) = create_child context metadata leaf_info.reservation_override
				virtual_size parent_location parent_ptr is_attached_rw in 
			Id_map.remap_id context metadata id (PVhd vhd1.vhduid);

			reattach context metadata id;

			debug "Creating new leaf for new VDI";
			let (vhd2,lvsize2) = create_child context metadata new_reservation_override virtual_size parent_location parent_ptr false in 
			let new_id2 = Uuidm.to_string (Uuidm.create Uuidm.(`V4)) in

			let (new_vsize,hidden) =
				match parent_ptr with
					| PVhd parent_vhd_uid ->
						let parent_vhd = Vhd_records.get_vhd context metadata.m_data.m_vhds parent_vhd_uid in

						(* Sort out the parent now - it's not being written to any more, so it's safe to query its size *)
						let (hidden,new_size) = set_hidden context metadata parent_vhd in
						let fixed_parent_vhd = {parent_vhd with size=new_size; hidden=hidden} in

						let need_reattach = resize_vhd context metadata fixed_parent_vhd None None in

						if need_reattach then reattach context metadata id;

						(* At this point, we hand over responsibility for the parent to coalesce *)
						Vhd_records.update_vhd_size context metadata.m_data.m_vhds parent_vhd_uid new_size;
						Vhd_records.update_hidden context metadata.m_data.m_vhds (PVhd parent_vhd_uid) hidden;

						debug "Marked vhd: %s as hidden=%d" parent_vhd_uid hidden;
						if hidden=2 then Coalesce.relink_children context metadata parent_vhd_uid;

						(Vhdutil.get_virtual_size fixed_parent_vhd.size, hidden)
					| PRaw parent_lv ->
						ignore(Locking.with_container_write_lock context metadata
							(fun container ->
								 (Lvmabs.add_tag context container parent_lv (Lvm.Tag.of_string "hidden"),())));
						(virtual_size,1)
			in

			(* Note at this point, the original vhd has disappeared *)

			Id_map.add_to_id_map context metadata new_id2 (PVhd vhd2.vhduid) new_reservation_override;

			new_id2)
