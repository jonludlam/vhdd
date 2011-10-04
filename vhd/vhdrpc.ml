module D=Debug.Debugger(struct let name="vhdrpc" end)
open D

let get_headers host path content_length task_id =
	[ 
		Printf.sprintf "POST %s HTTP/1.0" path;
		Printf.sprintf "User-Agent: vhdd/0.1";
		Printf.sprintf "Host: %s" host;
		"Content-Type: application/json";
		Printf.sprintf "Content-length: %d" content_length;
		Printf.sprintf "%s: %s" Http.Hdr.task_id task_id]
		
let rpc path call transport task_id =
  let open Xmlrpc_client in
      let str = Jsonrpc.to_string call in
      let http = xmlrpc ~version:"1.0" ~keep_alive:false ~body:str ~task_id ~length:(Int64.of_int (String.length str)) path in
      Jsonrpc.of_string (with_transport transport (fun fd ->
	Http_client.rpc fd http (fun response fd -> 
	  match response.Http.Response.content_length with
            | Some len ->
	      Unixext.really_read_string fd (Int64.to_int len)
	    | None ->
	      failwith "No content_length on response"
	)))
	  

let local_rpc : ((string -> Rpc.t -> Rpc.t) ref) = ref
  (fun task_id call ->
    try
      let path = Printf.sprintf "/fd_dispatch/vhdd/internal?pool_secret=%s" (Http.urlencode (Global.get_pool_secret ())) in
      rpc path call (Xmlrpc_client.TCP ("localhost",80)) task_id
    with e ->
      log_backtrace ();
      debug "Caught exception: %s" (Printexc.to_string e);
      raise e
  )

let remote_rpc task_id host port call =
  if !Global.unsafe_mode then
    let path = "/internal" in
    rpc path call (Xmlrpc_client.TCP (host,port)) task_id
  else
    let rec inner n =
      try 
	let path = Printf.sprintf "/fd_dispatch/vhdd/internal?pool_secret=%s" (Http.urlencode (Global.get_pool_secret ())) in
	rpc path call (Xmlrpc_client.TCP (host,port)) task_id
      with
	| Http_client.Http_error("401",_) as e ->
	  if n>0 (* Only retry once - we're only expecting this sort of failure after a pool join *)
	  then raise e 
	  else begin 
					(* Reread pool secret please! *)
	    Global.pool_secret := None;
	    inner (n+1)
	  end 
	| e ->
	  debug "Caught exception in remote_rpc: %s" (Printexc.to_string e);
	  raise e
    in
    inner 0

