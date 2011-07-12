(* Named mutex *)
open Threadext
open Int_types
module D=Debug.Debugger(struct let name="nmutex" end) 
open D

let waiting_enabled = ref false
let waiting_list : (string, lcontext) Hashtbl.t  = Hashtbl.create 10
let waiting_mutex = Mutex.create ()
let waiting_condition = Condition.create ()

(* Waiting list stuff *)
let wait ctx reason lock_required =
	debug "wait: reason=%s" reason;
	let context = {lc_context=ctx; lc_blocked=true; lc_reason=reason; lc_lock_required=lock_required} in
	if !waiting_enabled then begin
		Mutex.execute waiting_mutex (fun () ->
			let uuid = Uuid.to_string (Uuid.make_uuid ()) in
			Hashtbl.replace waiting_list uuid context;
			while context.lc_blocked do
				Condition.wait waiting_condition waiting_mutex
			done;
			Hashtbl.remove waiting_list uuid
		)
	end

let get_waiting_list () =
	Mutex.execute waiting_mutex (fun () ->
		Hashtbl.fold (fun k v acc -> if v.lc_blocked then (k,v)::acc else acc) waiting_list [])

let unwait uuid =
	Mutex.execute waiting_mutex (fun () ->
		let context = Hashtbl.find waiting_list uuid in
		context.lc_blocked <- false;
		Condition.broadcast waiting_condition)

let set_waiting_mode = (:=) waiting_enabled

type t = { 
	m : Mutex.t;
	mn : string;
}

type cond = {
	c : Condition.t;
	cn : string;
}

let rpc_of_t t =
	Rpc.rpc_of_string t.mn

let t_of_rpc rpc =
	{ m=Mutex.create ();
	  mn=Rpc.string_of_rpc rpc }

let rpc_of_cond t =
	Rpc.rpc_of_string t.cn

let cond_of_rpc rpc =
	{ c = Condition.create ();
	  cn = Rpc.string_of_rpc rpc }

let create name = { 
	m=Mutex.create ();
	mn=name;
}

let create_condition name = {
	c=Condition.create ();
	cn=name;
}

let dummy_hook (context : Smapi_types.context) (reason : string) (lock : string) = ()
let dummy2_hook (context : Smapi_types.context) (reason : string) (cond : string) (lock : string) = ()

let lock_hook = ref dummy_hook
let unlock_hook = ref dummy_hook
let cond_wait_pre_hook = ref dummy2_hook
let cond_wait_post_hook = ref dummy2_hook
let cond_broadcast_hook = ref dummy_hook

let execute context t reason f = 
	wait context reason t.mn;
	Mutex.execute t.m (fun () ->
		(!lock_hook) context reason t.mn;
		Pervasiveext.finally 
			f 
			(fun () -> 
				(!unlock_hook) context reason t.mn))

let condition_wait context cond t =
	(!cond_wait_pre_hook) context "" cond.cn t.mn;
	Condition.wait cond.c t.m;
	(!cond_wait_post_hook) context "" cond.cn t.mn
		
let condition_broadcast context cond =
	(!cond_broadcast_hook) context "" cond.cn;
	debug "About to broadcast";
	Condition.broadcast cond.c;
	debug "Done"

	

