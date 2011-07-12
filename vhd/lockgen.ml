module type OPS =
	sig
		type operation
		val string_of_operation : operation -> string
		val required_serialisations : operation list list
		val rpc_of_operation : operation -> Rpc.t
		val operation_of_rpc : Rpc.t -> operation
	end



module Lock =
	functor (O : OPS) ->
		struct

			type oplist = O.operation list 

			and state = {
				lock : Nmutex.t;
				cond : Nmutex.cond;
				mutable cur : oplist
			} with rpc

			exception OpConflict

			let create lockname condname =
				{
					lock = Nmutex.create lockname;
					cond = Nmutex.create_condition condname;
					cur = []
				}

			let to_string context state =
				Nmutex.execute context state.lock "Dumping" (fun () ->
					Jsonrpc.to_string (rpc_of_oplist state.cur))

			(* This function assumes the lock is held *)
			let find_conflicting_ops state op =
				List.filter (fun op2 ->
					List.exists (fun oplist ->
						List.mem op oplist && List.mem op2 oplist) O.required_serialisations
				) state.cur

			(* Assumes lock is being held *)
			let remove_op state op =
				state.cur <-
					let (found,list) = List.fold_left
						(fun (found,acc) elt ->
							if found then (found,elt::acc) else
								if elt=op
								then (true,acc)
								else (false,elt::acc)
						) (false,[]) state.cur
					in
					if not found then failwith "Didn't find the op!" else list

			let with_op_inner wait context state op f =
				let msg = Printf.sprintf "Executing with operation '%s'" (O.string_of_operation op) in
				Nmutex.execute context state.lock msg (fun () ->
					while List.length (find_conflicting_ops state op) > 0 do
						if wait 
						then Nmutex.condition_wait context state.cond state.lock 
						else raise OpConflict
					done;
					state.cur <- op :: state.cur);
				Pervasiveext.finally f
					(fun () -> let msg = Printf.sprintf "Finished op '%s'. Removing from cur" (O.string_of_operation op) in
					Nmutex.execute context state.lock msg (fun () ->
						remove_op state op;
						Nmutex.condition_broadcast context state.cond))

			let with_op context state op f = with_op_inner true context state op f
			let with_op_nowait context state op f = with_op_inner false context state op f

		end

