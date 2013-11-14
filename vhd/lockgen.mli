(** Lock generator 
	
	Given an operation type and a list of lists of conflicting operations,
	this functor will produce a module that contains a function 'with_op'
	which will atomically check for any currently-running conflicting 
	operations, and if there are none, adds the operation to the state and
	calls the function passed to it.

*)


(** The operation type module
	
	As an example, consider the case of operation 'A', 'B', 'C' and 'D'.
	If you want A and B to run concurrently, but 'C' and 'D' never to 
	happen at the same time, the module would look like:

	module Example = struct
	  type operation = | A | B | C | D
      let required_serialisations = [ [ C; D ] ]
	end

	In this case, all operations would run concurrently except C and D
	which would be serialised with respect to each other.  *)
module type OPS =
  sig
    type operation
    val string_of_operation : operation -> string
    val required_serialisations : operation list list
    val rpc_of_operation : operation -> Rpc.t
    val operation_of_rpc : Rpc.t -> operation
  end


(** The lock module *)
module Lock :
  functor (O : OPS) ->
    sig
	  (** state is the shared data structure in which the locks live *)
      type state

      val state_of_rpc : Rpc.t -> state
      val rpc_of_state : state -> Rpc.t

	  (** This is the type of exception that is thrown in 'with_op_nowait' *)
      exception OpConflict

	  (** Construct the shared state type *)
      val create : string -> string -> state

	  (** Create a string representation of the state *)
      val to_string : Context.t -> state -> string

	  (** Apply a function when no conflicting operations are in progress. Will 
		  block while there is a conflict *)
      val with_op :
        Context.t -> state -> O.operation -> (unit -> 'a) -> 'a

	  (** Apply a function when no conflicting operations are in progress. Will throw
		  'OpConflict' if there is a conflict *)
      val with_op_nowait :
        Context.t -> state -> O.operation -> (unit -> 'a) -> 'a
    end
