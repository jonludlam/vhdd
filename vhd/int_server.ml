(* Internal handler *)
open Int_rpc
open Int_types
open Threadext

module S = Int_rpc.Server(Vhdsm)

let local_rpc task_id call =
  let context = Context.({c_driver="unknown"; c_api_call=""; c_task_id=task_id; c_other_info=[]}) in
  S.process context call

