open Int_types
open Vhd_types
open Smapi_types
open Threadext
open Vhd_records
module D=Debug.Debugger(struct let name="tracelog" end)
open D

type smapicall_ty = { path : string; body: string; call: string }
and smapiresult_ty = { result: string }
and tracelog_data = 
(* Master state marshalling *)
	| Master_m_vhd_container of Lvmabs_types.container_info
	| Master_m_id_to_leaf_mapping of (string, leaf_info) Hashtbl.t
	| Master_m_vhds of Vhd_records.vhd_record_container
	| Master_m_attached_hosts of slave_sr_attachment list
	| Master_m_coalesce_in_progress of bool
	| Master_m_rolling_upgrade of bool

(* And individual updates *)
	| Master_id_to_leaf_update of string * leaf_info
	| Master_id_to_leaf_add of string * leaf_info
	| Master_id_to_leaf_remove of string
	| Master_create_lv of string * Int_types.lv_attach_info_t
	| Master_resize_lv of string * Int_types.lv_attach_info_t
	| Master_remove_lv of string
	| Master_lv_add_tag of string * string
	| Master_lv_remove_tag of string * string

(* Slave state marshalling *)
	| Slave_s_master of host option
	| Slave_s_current_ops of (string, slave_op) Hashtbl.t
	| Slave_s_master_approved_ops of (string, slave_op) Hashtbl.t
	| Slave_s_attached_vdis of (string, slave_attached_vdi_info) Hashtbl.t
	| Slave_s_ready of bool
	| Slave_s_thin_provision_request_in_progress of bool

(* individual updates *)
	| Slave_s_current_ops_add of (string * slave_op)
	| Slave_s_current_ops_remove of string
	| Slave_s_attached_vdis_add of (string * slave_attached_vdi_info)
	| Slave_s_attached_vdis_remove of string
	| Slave_s_attached_vdis_update of (string * slave_attached_vdi_info)
	| Slave_s_master_approved_ops_add of (string * slave_op)
	| Slave_s_master_approved_ops_remove of string

	| LockTaken of string
	| LockReleased of string
	| Master_to_slave_call of Rpc.t
	| Master_to_slave_response of Rpc.t
	| Slave_to_master_call of Rpc.t
	| Slave_to_master_response of Rpc.t

	| SmapiCall of smapicall_ty
	| SmapiResult of smapiresult_ty
	| InternalCall of string * string
	| InternalResult of string

and tracelog_entry = {
	thread_id : int;
	task_id : string;
	data : tracelog_data;
	timestamp : float;
	message : string option;
	other_info : (string * string) list
}

and current_log = {
	mutable log : tracelog_entry list;
} with rpc

let log={log=[]}
let lock = Mutex.create ()

let add_to_c_other_info context key value =
	context.c_other_info <- (key,value)::(List.remove_assoc key context.c_other_info)
let remove_from_c_other_info context key =
	context.c_other_info <- List.remove_assoc key context.c_other_info

let enabled = ref false 
let maxsize=ref 0
let maxdata=ref (InternalResult "")

let append context data message = 
	if !enabled then Mutex.execute lock (fun () ->
		let entry = {
			thread_id = Thread.id (Thread.self ());
			task_id=context.c_task_id;
			data = data;
			timestamp = Unix.gettimeofday ();
			message = message;
			other_info = context.c_other_info;
		} in
		let size = String.length (Jsonrpc.to_string (rpc_of_tracelog_entry entry)) in
		debug "size: %d (entry type: %s)" size (Jsonrpc.to_string (rpc_of_tracelog_data data));
		if size > !maxsize then begin
			maxsize := size;
			maxdata := data
		end;
		log.log <- entry :: log.log)

let get_txt () =
	let rpc = Mutex.execute lock (fun () -> rpc_of_current_log log) in
	let txt = Jsonrpc.to_string rpc in
	txt

let dump filename =
	Unixext.write_string_to_file filename (Jsonrpc.to_string (rpc_of_tracelog_data !maxdata))


(* For nmutex *)

let init () =
	let lock_hook context reason lock =
		append context (LockTaken lock) (Some reason)
	in
	let unlock_hook context reason lock = 
		append context (LockReleased lock) (*(Some reason)*) None
	in
	let cond_wait_pre_hook context reason cond lock =
		append context (LockReleased lock) (Some (Printf.sprintf "Waiting on condition: %s" cond))
	in
	let cond_wait_post_hook context reason cond lock =
		append context (LockTaken lock) (Some (Printf.sprintf "Woken up on condition: %s" cond))
	in
	Nmutex.lock_hook := lock_hook;
	Nmutex.unlock_hook := unlock_hook;
	Nmutex.cond_wait_pre_hook := cond_wait_pre_hook;
	Nmutex.cond_wait_post_hook := cond_wait_post_hook

let tracelog_handler req fd =
	req.Http.close <- true;
	let str = get_txt () in
	Http_svr.response_str req fd str
