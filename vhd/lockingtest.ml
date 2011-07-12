open Threadext
open Smapi_types

module SC=Smapi_client
module D=Debug.Debugger(struct let name="parallel_test" end)
open D

let measure_call intrpc fn id =
	let finished = ref false in
	let m = Mutex.create () in
	let test_thread () =
		fn id;
		Mutex.execute m (fun () -> finished := true)
	in
	Thread.create test_thread ();
	let rec inner n =
		if Mutex.execute m (fun () -> !finished) then () else begin
			let waiting=Int_client.Debug.waiting_locks_get intrpc in
			if not (List.exists (fun (k,v) -> v.lc_context.c_task_id=id) waiting) then begin
				Thread.delay 0.1;
				inner n
			end else begin
				let (lock,_) = List.find (fun (k,v) -> v.lc_context.c_task_id=id) waiting in
				Int_client.Debug.waiting_lock_unwait intrpc lock;
				inner (n+1)
			end
		end
	in
	let n = inner 0 in
	Printf.printf "id: %s took %d ops" id n

let lockingtest rpc testrpc =
	Int_client.Debug.waiting_mode_set intrpc true;

	let create () = ignore(SC.VDI.create (testrpc "vdi_create") gp sr [] (Int64.mul meg 50L)).vdi_info_location in

	measure_call (intrpc "test") create "vdi_create"

(*  let tasks = Int_client.Debug.waiting_locks_get intrpc in

	List.map (fun (k,v) -> Printf.printf "lock: %s  context: {lc_reason: '%s'; lc_lock_required: %s; api_call: '%s'; task_id: '%s'}" k v.lc_reason v.lc_lock_required v.lc_context.c_api_call v.lc_context.c_task_id) tasks;*)
