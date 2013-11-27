open Threadext
open Int_types
open Context

module D=Debug.Make(struct let name="parallel_test" end)
open D

let meg = Int64.mul 1024L 1024L
let measure_call intrpc fn response id =
	let finished = ref false in
	let m = Mutex.create () in
	let test_thread () =
		response(fn id);
		Mutex.execute m (fun () -> finished := true)
	in
	ignore(Thread.create test_thread ());
	let rec inner n =
		if Mutex.execute m (fun () -> !finished) then n else begin
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
	Printf.printf "id: %s took %d ops\n" id n;
	n
    
(*let lockingtest rpc testrpc intrpc gp sr =
  Int_client.Debug.waiting_mode_set intrpc true;
  
  let created = ref "" in
  let create id = (Client.VDI.create (testrpc id) gp sr [] (Int64.mul meg 50L)).vdi_location in
  let create_response loc = created := loc in
  measure_call intrpc create create_response "vdi_create";

  let cloned = ref "" in
  let clone id = (SC.VDI.clone (testrpc id) gp sr [] !created).vdi_location in
  let clone_response loc = cloned := loc in
  measure_call intrpc clone clone_response "vdi_clone";

  let delete id = SC.VDI.delete (testrpc id) gp sr !created in
  let delete_response () = () in
  measure_call intrpc delete delete_response "vdi_delete";

  let attach id = SC.VDI.attach (testrpc id) gp sr !cloned true in
  let attach_response _ = () in
  measure_call intrpc attach attach_response "vdi_attach";

  let activate id = SC.VDI.activate (testrpc id) gp sr !cloned in
  measure_call intrpc activate attach_response "vdi_activate";

  let deactivate id = SC.VDI.deactivate (testrpc id) gp sr !cloned in
  measure_call intrpc deactivate attach_response "vdi_deactivate";

  let detach id = SC.VDI.detach (testrpc id) gp sr !cloned in
  measure_call intrpc detach attach_response "vdi_detach";

  Int_client.Debug.waiting_mode_set intrpc false;
  ignore(Thread.create (fun () -> debug "Got response (1): %s" (SC.VDI.attach (testrpc "1") gp sr !cloned true)) ());
  ignore(Thread.create (fun () -> debug "Got response (2): %s" (SC.VDI.attach (testrpc "2") gp sr !cloned true)) ());
  ignore(Thread.create (fun () -> debug "Got response (3): %s" (SC.VDI.attach (testrpc "3") gp sr !cloned true)) ());

  SC.VDI.detach (testrpc "") gp sr !cloned;

  ()
*)
    

    
(*  let tasks = Int_client.Debug.waiting_locks_get intrpc in

    List.map (fun (k,v) -> Printf.printf "lock: %s  context: {lc_reason: '%s'; lc_lock_required: %s; api_call: '%s'; task_id: '%s'}" k v.lc_reason v.lc_lock_required v.lc_context.c_api_call v.lc_context.c_task_id) tasks;*)
