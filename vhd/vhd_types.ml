
open Int_types
open Vhd_records

(*type attachment = {
  a_host : host;
  a_leaf_path : string;
  }*)


type operation_ty =
	| OpDelete
	| OpClone
	| OpResize

	| OpActivating
	| OpDeactivating
	| OpLeafCoalesceStopAndCopy

	| OpAttaching
	| OpDetaching
	| OpReattaching
with rpc

module MasterOperations = struct
  type operation = operation_ty

  let string_of_operation op = Jsonrpc.to_string (rpc_of_operation_ty op)

  let required_serialisations = 
    [ [ OpActivating; OpDeactivating; OpLeafCoalesceStopAndCopy];
      [ OpAttaching; OpDetaching; OpReattaching; OpDelete; OpResize; OpLeafCoalesceStopAndCopy ];
      [ OpClone; OpResize; OpDelete ] ]

  let rpc_of_operation = rpc_of_operation_ty
  let operation_of_rpc = operation_ty_of_rpc
end

module MLock = Lockgen.Lock(MasterOperations)

type attachment_ty =
	| AttachedRO of string list
	| AttachedRW of string list

and activation_ty =
	| ActiveRO of string list
	| ActiveRW of string
	| ActiveRWRaw of string list

and smapiv2_info_ty = {
	content_id : Storage_interface.content_id;
	name_label : string;
	name_description : string;
	ty : string;
	metadata_of_pool : string;
	is_a_snapshot : bool;
	snapshot_time : string;
	snapshot_of : Storage_interface.vdi;
	read_only : bool;
	persistent : bool;
	sm_config : (string * string) list;
}

and leaf_info = {
	current_operations : MLock.state; 
	attachment : attachment_ty option;
	active_on : activation_ty option;
	reservation_override : Vhdutil.reservation_mode option;
	smapiv2_info : smapiv2_info_ty;
	leaf : pointer;
}

and slave_sr_attachment = {
	ssa_host : host;
	mutable ssa_resync_required : bool;
}

and slave_sr_attachment_info = {
	l : slave_sr_attachment list;
	master : host option;
}

with rpc

type id_to_leaf_mapping_t = (string, leaf_info) Hashtbl.t
and vhds_t = (string,vhd_record) Hashtbl.t
and m_attached_hosts_t = slave_sr_attachment list

and master_sr_metadata_data = {
	(* LVM metadata / VG name / Filesystem path *)
	mutable m_vhd_container : Lvmabs_types.container_info;

	(* Mapping of identifier to leaf *)
	m_id_mapping_persistent_store : (Lvmabs_types.container_info * (Lvmabs_types.location_info option));
	m_id_to_leaf_mapping : id_to_leaf_mapping_t; (* This is mutable. *)

	(* VHD metadata *)
	m_vhds : Vhd_records.vhd_record_container;

	(* Which hosts are attached to this SR *)
	m_attached_hosts_persistent_store : (Lvmabs_types.container_info * Lvmabs_types.location_info);
	mutable m_attached_hosts : m_attached_hosts_t;

	(* Is there a coalesce currently in progress? *)
	mutable m_coalesce_in_progress : bool;

	(* Type of LVM sizing policy *)
	m_lvm_reservation_mode : Vhdutil.reservation_mode option;

	(* Our uuid *)
	m_sr_uuid : string;

	(* Rolling upgrade mode *)
	mutable m_rolling_upgrade : bool;

	(* Completely finished all the post-attach stuff *)
	mutable m_attach_finished : bool;
} with rpc

type master_sr_metadata = {
	(* The actual data *)
	m_data : master_sr_metadata_data;
	mutable m_idx : int;

	(* Protects m_vhd_container *)
	m_container_lock : Rwlock.t;

	(* Protects and signals use of m_id_to_leaf_mapping *)
	m_id_mapping_lock : Nmutex.t;
	m_id_mapping_condition : Nmutex.cond;

	(* Protects m_vhds *)
	m_vhd_hashtbl_lock : Nmutex.t;

	(* protects m_attached_hosts *)
	m_attached_hosts_lock : Nmutex.t;
	m_attached_hosts_condition : Nmutex.cond;

	(* protects m_coalesce_in_progress - not necessary, maybe? *)
	m_coalesce_in_progress_lock : Nmutex.t;
	m_coalesce_condition : Nmutex.cond;
} 

type slave_attached_vdi_info = {
	savi_attach_info : slave_attach_info;
	savi_blktap2_dev : Tapctl.tapdev;
	savi_resync_required : bool;
	savi_endpoint : string;
	savi_link : string;
	savi_phys_size : int64;
	savi_maxsize : int64 option;
	mutable savi_activated : bool;
	mutable savi_paused : bool;
}

and slave_op = | Attaching
			   | Reattaching
			   | Detaching
			   | Activating
			   | Deactivating
			   | AttachAndActivating
			   | LeafCoalescing 
			   | Reactivating

and attached_vdis_t = (string, slave_attached_vdi_info) Hashtbl.t
and slave_sr_metadata_data = {
	s_path : string;
	mutable s_master : host option;
	s_current_ops : (string, slave_op) Hashtbl.t;
	s_master_approved_ops : (string, slave_op) Hashtbl.t;
	s_attached_vdis : attached_vdis_t;
	mutable s_ready : bool;
	s_sr : Storage_interface.sr;
	s_thin_provisioning : bool;
	mutable s_thin_provision_request_in_progress : bool;
} with rpc

type slave_sr_metadata = {
	mutable s_idx : int;
	s_mutex : Nmutex.t;
	s_condition : Nmutex.cond;
	mutable s_rpc : string -> Rpc.t -> Rpc.t;
	s_data : slave_sr_metadata_data;
} 


(* Safe helper functions *)

module Master_helpers = struct

	let safe_get_leaf_info context metadata id =
		Nmutex.execute context metadata.m_id_mapping_lock "Getting leaf info"
			(fun () -> Hashtbl.find metadata.m_data.m_id_to_leaf_mapping id)

end

let attachments_to_string leaf_info =
	match leaf_info.attachment with
		| None -> "None"
		| Some (AttachedRO hosts) -> Printf.sprintf "Attached RO: (%s)"  (String.concat "," hosts)
		| Some (AttachedRW hosts) -> Printf.sprintf "Attached RW: (%s)"  (String.concat "," hosts)

let activations_to_string leaf_info =
	match leaf_info.active_on with
		| Some (ActiveRO hosts) -> Printf.sprintf "ActiveRO: (%s)" (String.concat "," hosts)
		| Some (ActiveRW host) -> Printf.sprintf "ActiveRW: %s" host
		| Some (ActiveRWRaw hosts) -> Printf.sprintf "ActiveRWRaw: (%s)" (String.concat "," hosts)
		| None -> "None"

let current_ops_to_string context state =
	MLock.to_string context state

