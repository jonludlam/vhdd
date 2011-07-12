open Int_types

module I=struct
	type t=int64 with rpc
	let add=Int64.add
	let sub=Int64.sub
end

module Int64extentlist = Extentlist.Extentlist(I)
type junk_t = (Int64extentlist.t * char) list with rpc


type vdi_id = string
and token = string
and sr_uuid = string	
and host_uuid = string with rpc

type intrpc = 
	| Sr_slave_attach of token * sr_uuid * host * ((vdi_id * bool) list)
	| Sr_slave_detach of token * sr_uuid * host
	| Sr_slave_recover of token * sr_uuid * host
	| Sr_mode of sr_uuid 
	| Sr_thin_provision_check of sr_uuid

	| Vdi_slave_attach of host_uuid * sr_uuid * vdi_id * bool * bool
	| Vdi_get_slave_attach_info of sr_uuid * vdi_id
	| Vdi_slave_detach of host_uuid * sr_uuid * vdi_id
	| Vdi_slave_activate of host_uuid * sr_uuid * vdi_id * bool
	| Vdi_slave_deactivate of host_uuid * sr_uuid * vdi_id 
	| Vdi_slave_reload of sr_uuid * ((vdi_id * slave_attach_info) list)
	| Vdi_slave_leaf_coalesce_stop_and_copy of sr_uuid * vdi_id * string * string
	| Vdi_slave_set_phys_size of sr_uuid * vdi_id * int64
(*	| Vdi_thin_provision_request_more_space of sr_uuid * host_uuid * ((vdi_id * string * int64) list)*)
	
	| Host_set_dead of host_uuid
	| Host_rolling_upgrade_finished

	| Debug_waiting_locks_get 
	| Debug_waiting_lock_unwait of string
	| Debug_waiting_mode_set of bool
	| Debug_vdi_get_leaf_path of sr_uuid * vdi_id
	| Debug_get_tracelog of host_uuid
	| Debug_die of bool

	| Debug_get_id_to_leaf_map of sr_uuid
	| Debug_get_vhds of sr_uuid
	| Debug_get_master_metadata of sr_uuid
	| Debug_get_vhd_container of sr_uuid
	| Debug_get_attached_vdis of sr_uuid
	| Debug_get_slave_metadata of sr_uuid
	| Debug_get_pid
	| Debug_get_attach_finished of sr_uuid 
	| Debug_slave_get_leaf_vhduid of sr_uuid * vdi_id
	| Debug_write_junk of sr_uuid * vdi_id * int64 * int * junk_t
	| Debug_check_junk of sr_uuid * vdi_id * junk_t
	| Debug_get_host 
	| Debug_get_ready 

and intrpc_response =
	| Nil
	| String of string
	| Sr_mode_response of attach_mode
	| Slave_attach_info of slave_attach_info
	| Waiting_locks of (string * lcontext) list
	| Tracelog of Rpc.t 
	| Lvs of dm_node_info_list
	| Id_to_leaf_map of Rpc.t
	| Vhds of Rpc.t
	| Attached_vdis of Rpc.t
	| Master_metadata of Rpc.t
	| Slave_metadata of Rpc.t
	| Vhd_container of Rpc.t
	| Vhdd_pid of int
	| Attach_finished of bool
	| Vhduid of string
	| Junk of junk_t
	| Host of string

and intrpc_response_wrapper = 
	| Success of intrpc_response
	| Failure of string * (string list)

with rpc

let wrap_rpc rpc call =
	intrpc_response_wrapper_of_rpc (rpc (rpc_of_intrpc call))

(*let local_intrpc task_id call = 
	wrap_rpc (!Vhdrpc.local_rpc task_id) call

let remote_intrpc task_id host port call = wrap_rpc (Vhdrpc.remote_rpc task_id host port) call
*)

