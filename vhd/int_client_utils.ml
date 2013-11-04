open Vhd_types
open Int_types
open Int_rpc

module D=Debug.Make(struct let name="int_client_utils" end)
open D

let rec slave_retry_loop context allowed_errors (f : (intrpc -> intrpc_response_wrapper) -> 'a) metadata : 'a =
	try
		let rpc call = 
			Tracelog.append context (Tracelog.Slave_to_master_call (rpc_of_intrpc call)) None;
			let result = (Int_rpc.wrap_rpc (metadata.s_rpc context.c_task_id)) call in
			Tracelog.append context (Tracelog.Slave_to_master_response (rpc_of_intrpc_response_wrapper result)) None;
			result
			in
		f rpc
	with
		| IntError(e,args) as exn ->
			if List.mem e allowed_errors
			then (debug "Ignorning allowed error '%s' in slave_retry_loop" e; raise exn)
			else begin
				log_backtrace ();
				error "Unexpected internal exception in slave_retry_loop: %s,[%s]" e (String.concat "; " args);
				Thread.delay 1.0;
				slave_retry_loop context allowed_errors f metadata
			end
		| e ->
			error "Unexpected error in cleanup: %s" (Printexc.to_string e);
			Thread.delay 1.0;
			slave_retry_loop context allowed_errors f metadata

let rec master_retry_loop context allowed_errors bad_errors f metadata host =
	try
		let attached_hosts = Slave_sr_attachments.get_attached_hosts context metadata in
		if not (List.exists (fun ssa -> ssa.ssa_host.h_uuid=host.h_uuid) attached_hosts) then
			raise (IntError(e_sr_not_attached,[host.h_uuid]));
		let rpc call = 
			Tracelog.append context (Tracelog.Master_to_slave_call (rpc_of_intrpc call)) None;
			let result = Int_rpc.wrap_rpc (Vhdrpc.remote_rpc context.c_task_id host.h_ip host.h_port) call in
			Tracelog.append context (Tracelog.Master_to_slave_response (rpc_of_intrpc_response_wrapper result)) None;
			result
		in
		f rpc
	with
		| IntError(e,args) as exn ->
			if List.mem e bad_errors then begin
				error "Bad error caught: not continuing";
				raise exn
			end;
			if List.mem e allowed_errors
			then debug "Ignorning allowed error '%s' in master_retry_loop" e
			else begin
				log_backtrace ();
				error "Unexpected internal exception in master_retry_loop: %s,[%s]" e (String.concat "; " args);
				Thread.delay 1.0;
				master_retry_loop context allowed_errors bad_errors f metadata host
			end
		| e ->
			error "Unexpected error in master_retry_loop: %s" (Printexc.to_string e);
			Thread.delay 1.0;
			master_retry_loop context allowed_errors bad_errors f metadata host
