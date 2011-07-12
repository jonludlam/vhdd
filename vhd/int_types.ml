open Smapi_types
open Camldm

exception IntError of (string * (string list))

let e_not_attached = "not_attached"
let e_not_activated = "not_activated"
let e_sr_not_attached = "host_not_attached"
let e_unknown_location = "unknown_location"
let e_vdi_not_attached = "vdi_not_attached"
let e_vdi_active_elsewhere = "vdi_active_elsewhere"

type dm_node_info = {
	dmn_dm_name : string;
	dmn_mapping : mapping_array
}
and dm_node_info_list = dm_node_info list
and lv_attach_info_t =
	| Mlvm of dm_node_info

and slave_attach_info = {
	sa_leaf_path : string;
	sa_leaf_maxsize : int64 option;
	sa_leaf_phys_size : int64;
	sa_leaf_is_raw : bool;
	sa_writable : bool;
	sa_lvs : lv_attach_info_t list;
} with rpc

type host = {
	h_uuid : string;
	h_ip : string;
	h_port : int;
} with rpc

type attach_mode =
	| Master | Slave of host option
with rpc

(** Context, used for information only *)
type lcontext = {
	lc_context : context;
	mutable lc_blocked : bool;
	lc_reason : string;
	lc_lock_required : string;
} with rpc


let string_of_lv_attach_info_t t =
	match t with
		| Mlvm dm_node_info ->
			Printf.sprintf "Mlvm (%s,%s)" dm_node_info.dmn_dm_name (Camldm.to_string dm_node_info.dmn_mapping.m)
