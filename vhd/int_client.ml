(* Minimal client for inter-daemon RPCs *)
open Int_types

module LocalClient = Int_rpc.Client (struct let rpc call = !Vhdrpc.local_rpc "" call end)
module type CLIENT = module type of LocalClient

exception NoIp

let get host = 
  if host.h_uuid = Global.get_host_uuid () 
  then (module LocalClient : CLIENT) 
  else
    match host.h_ip with 
    | Some ip -> 
      let rpc = Vhdrpc.remote_rpc "" ip host.h_port in
      (module (Int_rpc.Client (struct let rpc = rpc end)) : CLIENT)
    | None -> raise NoIp

