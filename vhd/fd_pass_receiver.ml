open Smapi_types

module D=Debug.Debugger(struct let name="fd_pass_receiver" end)
open D

let get_dir_path () = Printf.sprintf "%s/var/xapi/fd_servers/vhdd" (Global.get_host_local_dummydir ())

let start path handler =
	let dir_path = get_dir_path () in
	let fd_sock_path = Printf.sprintf "%s%s" dir_path path in
	Unixext.mkdir_rec dir_path 0o700;
	let fd_sock = Fecomms.open_unix_domain_sock () in
	Unixext.unlink_safe fd_sock_path;
	Unix.bind fd_sock (Unix.ADDR_UNIX fd_sock_path);
	Unix.listen fd_sock 5;

	let rec loop () =
		let (_,_,_) = Unix.select [fd_sock] [] [] (-1.0) in
		let (fd_sock2,_) = Unix.accept fd_sock in
		let buffer = String.make 16384 '\000' in

		let (len,from,newfd) = try
			Unixext.recv_fd fd_sock2 buffer 0 16384 []
		with e ->
			Unix.close fd_sock2;
			raise e
		in

		Unix.close fd_sock2;

		ignore(Thread.create (fun () ->
			Pervasiveext.finally (fun () ->
				let s = String.sub buffer 0 len in
				if len=0 then failwith "Connection closed";
				Printf.printf "Received fd with message: %s\n%!" s;
				let req = Http.request_of_rpc (Jsonrpc.of_string buffer) in
				handler req newfd)
				(fun () -> Unix.close newfd)) ());

		loop ()
	in
	loop ()
