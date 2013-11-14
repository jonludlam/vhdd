
module D=Debug.Make(struct let name="lockgentest" end)
open D

module Myops = struct

	type operation = 
		| OpA
		| OpB
		| OpC
		| OpD
		| OpE
		| OpF
		| OpG
		| OpH
		| OpI
		| OpJ
	with rpc

	let string_of_operation op =
		Jsonrpc.to_string (rpc_of_operation op)

	let required_serialisations = 
		[
			[OpA; OpB; OpC; OpJ];
			[OpD; OpE; OpJ];
			[OpF; OpG];
			[OpH; OpI; OpJ];
		]
end


module MyopsLock = Lockgen.Lock(Myops)
open Myops

let _ =
	let get_random_op () =
		let i = Random.int 10 in
		match i with
			| 0 -> OpA | 1 -> OpB | 2 -> OpC | 3 -> OpD
			| 4 -> OpE | 5 -> OpF | 6 -> OpG | 7 -> OpH
			| 8 -> OpI | 9 -> OpJ | _ -> failwith "Uhh?"
	in

	let state = MyopsLock.create "mylock" "mycond" in
	let context = {
		Smapi_types.c_driver="none";
		c_api_call="none";
		c_task_id="none";
		c_other_info=[]}
	in

	Logs.reset_all [ "file:/tmp/test.log" ];

	let gen_hook ty context reason lock =
		debug "%s: reason='%s' lock='%s'" ty reason lock
	in
	let gen2_hook ty context reason cond lock =
		debug "%s: reason='%s' cond='%s' lock='%s'" ty reason cond lock
	in

	Nmutex.lock_hook := (gen_hook "lock");
	Nmutex.unlock_hook := (gen_hook "unlock");
	Nmutex.cond_wait_pre_hook := (gen2_hook "wait_pre");
	Nmutex.cond_wait_post_hook := (gen2_hook "wait_post");
	Nmutex.cond_broadcast_hook := (fun context reason cond -> debug "broadcast: reason='%s' cond='%s'" reason cond);

	let dump () =
		debug "dump: %s" (MyopsLock.to_string context state);
		if (Random.int 5 = 0) then Thread.delay 1.0
	in

	let start = Unix.gettimeofday () in

	let thread () =
		while (Unix.gettimeofday () -. start) < 60.0 do
			let op = get_random_op () in
			MyopsLock.with_op context state op dump
		done;
		debug "Finished"
	in

	for i=0 to 10 do
		ignore(Thread.create thread ());
	done;

	Thread.delay 70.0;

	debug "Finished"
