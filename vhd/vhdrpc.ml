module D=Debug.Debugger(struct let name="vhdrpc" end)
open D

let get_headers host path content_length task_id =
	[ 
		Printf.sprintf "POST %s HTTP/1.0" path;
		Printf.sprintf "User-Agent: vhdd/0.1";
		Printf.sprintf "Host: %s" host;
		"Content-Type: application/json";
		Printf.sprintf "Content-length: %d" content_length;
		Printf.sprintf "%s: %s" Http.task_id_hdr task_id]
		

let local_rpc : ((string -> Rpc.t -> Rpc.t) ref) = ref
	(fun task_id call ->
		try
			let str = Jsonrpc.to_string call in
			let path = Printf.sprintf "/fd_dispatch/vhdd/internal?pool_secret=%s" (Http.urlencode (Global.get_pool_secret ())) in
			let headers = get_headers "localhost" path (String.length str) task_id in
			let result = Xmlrpcclient.do_http_rpc "localhost" 80 headers str (fun content_length task_id fd -> Unixext.really_read_string fd content_length) in
			Jsonrpc.of_string result
		with e ->
			log_backtrace ();
			debug "Caught exception: %s" (Printexc.to_string e);
			raise e
	)

let remote_rpc task_id host port call =
	let str = Jsonrpc.to_string call in
	if !Global.unsafe_mode then
		let path = "/internal" in
		let headers = get_headers host path (String.length str) task_id in
		let result = Xmlrpcclient.do_http_rpc host port headers str 
			(fun content_length task_id fd -> 
				let ifd : int = Obj.magic fd in
				try 
					debug "Using fd: %d" ifd;
					let result = Unixext.really_read_string fd content_length in
					debug "Read everything: closing fd %d" ifd;
					result
				with e -> 
					debug "Caught exception: closing fd %d" ifd;
					raise e
			) in
		Jsonrpc.of_string result
	else
		let rec inner n =
		try 
			let path = Printf.sprintf "/fd_dispatch/vhdd/internal?pool_secret=%s" (Http.urlencode (Global.get_pool_secret ())) in
			let headers = get_headers host path (String.length str) task_id in
			let result = Xmlrpcclient.do_http_rpc host 80 headers str (fun content_length task_id fd -> Unixext.really_read_string fd content_length) in
			Jsonrpc.of_string result
		with
			| Xmlrpcclient.Http_error("401",_) as e ->
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

