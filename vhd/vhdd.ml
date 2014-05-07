(* VHDD *)
open Xstringext

module D=Debug.Make(struct let name="vhdd" end)
open D

let server = Http_svr.Server.empty ()

module S = Storage_interface.Server(Vhdsm)

let read_body req fd =
	let len = match req.Http.Request.content_length with Some x -> x | None -> failwith "Need a content length" in
	Unixext.really_read_string fd (Int64.to_int len)

let xmlrpc_handler req fd () =
	req.Http.Request.close <- true;
	let path = match String.split '/' req.Http.Request.uri with
		| x::ys -> List.hd (List.rev (x::ys))
		| _ -> failwith "Unknown path"
	in
    let all = req.Http.Request.cookie @ req.Http.Request.query in
	let path = 
		if List.mem_assoc "path" all then List.assoc "path" all else path in
	debug "path=%s" path;
	let body = read_body req fd in
	debug "Request: %s" body;

	let rpc = Xmlrpc.call_of_string body in

	let context = Context.({
		c_driver=path; 
		c_api_call=rpc.Rpc.name; 
		c_task_id=(match req.Http.Request.task with Some x -> x | None -> Uuidm.to_string (Uuidm.create Uuidm.(`V4))); 
		c_other_info=[]; }) in
	Tracelog.append context (Tracelog.SmapiCall {Tracelog.path=path; body=body; call=rpc.Rpc.name}) None;
	Debug.associate_thread_with_task context.Context.c_task_id;
	let result = S.process context rpc in
	let str = Xmlrpc.string_of_response result in
	Tracelog.append context (Tracelog.SmapiResult {Tracelog.result=str;}) None;
	Tracelog.dump "/tmp/tracelog";
	debug "Response: (omitted)";
	Http_svr.response_str req fd str

let internal_handler req fd () =
	req.Http.Request.close <- true;
	debug "Internal handler";
	let body = read_body req fd in
	let call = Jsonrpc.call_of_string body in
	debug "Call=%s" body;
	(* Extract some info from the XML before we pass it to process *)
(*	let call,args = XMLRPC.From.methodCall xml in*)
	let context = Context.({c_driver="unknown"; c_api_call=""; c_task_id=(match req.Http.Request.task with Some x -> x | None -> Uuidm.to_string (Uuidm.create Uuidm.(`V4))); c_other_info=[] }) in
	Debug.associate_thread_with_task context.Context.c_task_id;
	(*Tracelog.append context (Tracelog.InternalCall (body, call)) None;*)
	let result = Int_server.S.process context call in
	let str = Jsonrpc.string_of_response result in
(*	Tracelog.append context (Tracelog.InternalResult str) None;*)
	debug "Response: %s" str;
	Http_svr.response_str req fd str

let register name =
        let unixserver = Http_svr.Server.empty () in
	if not !Global.dummy then begin
		let unix_socket_path = Printf.sprintf "/var/lib/xcp/sm/%s" name in
		Unixext.mkdir_safe (Filename.dirname unix_socket_path) 0o700;
		Unixext.unlink_safe unix_socket_path;
		let domain_sock = Http_svr.bind (Unix.ADDR_UNIX(unix_socket_path)) "unix-rpc" in
		Http_svr.start unixserver domain_sock;
		Http_svr.Server.add_handler unixserver Http.Post "/" (Http_svr.FdIO (fun req fd _ -> 
		  let open Http.Request in 
		  let req = {req with uri = if req.uri = "/" then (Printf.sprintf "/%s" name) else req.uri} in
		  xmlrpc_handler req fd ()));
	end;
	Http_svr.Server.add_handler server Http.Post (Printf.sprintf "/%s" name) (Http_svr.FdIO (fun req fd _ -> xmlrpc_handler req fd ()))
	
let server_init () =
	Tapdisk_listen.start ();

	Tracelog.init ();

	(* Start up the HTTP internal and status handlers. This is required
	   since the reconnect phase requires communications between masters
	   and slaves. Happily, We only require internal API for this
	   though, so we delay the startup of the SMAPI handlers until we're
	   fully bootstrapped. *)
	Http_svr.Server.add_handler server Http.Get "/status" (Http_svr.FdIO Html.status_handler);
	Http_svr.Server.add_handler server Http.Get "/tracelog" (Http_svr.FdIO Tracelog.tracelog_handler);
	Http_svr.Server.add_handler server Http.Post "/unwait" (Http_svr.FdIO Html.wait_handler);
	Http_svr.Server.add_handler server Http.Get "/dot" (Http_svr.FdIO Html.dot_handler);
	Http_svr.Server.add_handler server Http.Get "/updates" (Http_svr.FdIO Html.update_handler);
	Http_svr.Server.add_handler server Http.Post "/internal" (Http_svr.FdIO internal_handler);
	Http_svr.Server.add_handler server Http.Post "/global" (Http_svr.FdIO internal_handler);

	if !Global.enable_fileserver then
	  Http_svr.Server.add_handler server Http.Get "/" (Http_svr.BufIO (Fileserver.send_file "/" !Global.fileserver_base));

	ignore(Thread.create (fun () -> Fd_pass_receiver.start "/debug" xmlrpc_handler) ());
	ignore(Thread.create (fun () -> Fd_pass_receiver.start "/internal" internal_handler) ());
	ignore(Thread.create (fun () -> Fd_pass_receiver.start "/tracelog" Tracelog.tracelog_handler) ());
	ignore(Thread.create (fun () -> Fd_pass_receiver.start "/status" Html.status_handler) ());
	ignore(Thread.create (fun () -> Fd_pass_receiver.start "/dot" Html.dot_handler) ());
	ignore(Thread.create (fun () -> Fd_pass_receiver.start "/updates" Html.update_handler) ());

	(*let localhost = Unix.inet_addr_of_string "127.0.0.1" in*)
	let localhost_sock = Http_svr.bind (Unix.ADDR_INET(Unix.inet_addr_any, !Global.port)) "inet-rpc" in
	Http_svr.start server localhost_sock;

	(* Reattach using the /var/run/vhdd/attachments.xml file *)

	let previously_attached = Attachments.read_attachments () in
	List.iter Vhdsm.SR.reattach previously_attached;

	let driver_names = Drivers.get_all_driver_names () in
	List.iter register driver_names;

	let queue_name = match !Global.host_uuid, !Global.dummy with
	  | Some h, true -> Printf.sprintf "org.xen.xcp.storage.local_%s" h
	  | _, _ -> "org.xen.xcp.storage.local"
	in

	let service = Xcp_service.make ~path:(!Storage_interface.default_path) ~queue_name
	  ~rpc_fn:(fun s -> S.process (Context.({c_driver="local"; c_api_call=""; c_task_id=""; c_other_info=[]})) s) () in
	ignore(Thread.create (fun () -> Xcp_service.serve_forever service) ());


	let queue_name = match !Global.host_uuid, !Global.dummy with
	  | Some h, true -> Printf.sprintf "org.xen.xcp.storage.lvmnew_%s" h
	  | _, _ -> "org.xen.xcp.storage.lvmnew"
	in

	let service = Xcp_service.make ~path:(!Storage_interface.default_path) ~queue_name
	  ~rpc_fn:(fun s -> S.process (Context.({c_driver="lvmnew"; c_api_call=""; c_task_id=""; c_other_info=[]})) s) () in
	ignore(Thread.create (fun () -> Xcp_service.serve_forever service) ());

	Global.ready := true;

	while true do
		Thread.delay 20000.;
	done

module MLVMDebug=Debug.Make(struct let name="mlvm" end)
module W=Debug.Make(struct let name="watchdog" end)

let delay_on_eintr f =
  try
    f ()
  with
    Unix.Unix_error(Unix.EINTR,_,_) ->
      debug "received EINTR. waiting to enable db thread to flush";
      Thread.delay 60.;
      exit(0)
  | e -> raise e

let watchdog f =
  if !Global.nowatchdog then delay_on_eintr f
  else
    begin
      (* parent process blocks sigint and forward sigterm to child. *)
      ignore(Unix.sigprocmask Unix.SIG_BLOCK [Sys.sigint]);
      Sys.catch_break false;

      (* watchdog logic *)
      let loginfo fmt = W.info fmt in

      let restart = ref true
      and error_msg = ref "" and exit_code = ref 0
			     and last_badsig = ref (0.) and pid = ref 0
							and last_badexit = ref (0.) and no_retry_interval = 60. in

      while !restart
      do
	begin
	  loginfo "(Re)starting vhdd...";
	  if !pid = 0 then
	    begin
	      let newpid = Unix.fork () in
	      if newpid = 0 then
		begin
		  try
		    ignore(Unix.sigprocmask Unix.SIG_UNBLOCK [Sys.sigint]);
		    delay_on_eintr f;
		    exit 127
		  with e ->
		    error "Caught exception at toplevel: '%s'" (Printexc.to_string e);
		    log_backtrace ();
		    raise e (* will exit the process with rc=2 *)
		end;
	      (* parent just reset the sighandler *)
	      Sys.set_signal Sys.sigterm (Sys.Signal_handle (fun i -> restart := false; Unix.kill newpid Sys.sigterm));
	      pid := newpid;
	    end;
	  try
	    loginfo "Child vhdd is: %d" !pid;
	    (* remove the pid in all case, except stop *)
	    match snd (Unix.waitpid [] !pid) with
	    | Unix.WEXITED i when i = Global.restart_return_code ->
		pid := 0;
		loginfo "restarting vhdd in different operating mode";
		()
	    | Unix.WEXITED i when i=0->
		loginfo "received exit code 0. Not restarting.";
		pid := 0;
		restart := false;
		error_msg := "";
	    | Unix.WEXITED i ->
		loginfo "received exit code %d" i;
		exit_code := i;
		pid := 0;
		let ctime = Unix.time () in
		if ctime < (!last_badexit +. no_retry_interval) then
		  begin
		    restart := false;
		    loginfo "Received 2 bad exits within no-retry-interval. Giving up.";
		  end
		else
		  begin
		    (* restart := true; -- don't need to do this - it's true already *)
		    loginfo "Received bad exit, retrying";
		    last_badexit := ctime
		  end
	    | Unix.WSIGNALED i ->
		loginfo "received signal %d" i;
		pid := 0;
		(* arbitrary choice of signals, probably need more
		   though, for real use *)
		if i = Sys.sigsegv || i = Sys.sigpipe then
		  begin
		    let ctime = Unix.time () in
		    if ctime < (!last_badsig +. no_retry_interval) then
		      begin
			restart := false;
			error_msg := Printf.sprintf "vhdd died with signal %d: not restarting (2 bad signals within no_retry_interval)" i;
			exit_code := 13
		      end else
			begin
			  loginfo "vhdd died with signal %d: restarting" i;
			  last_badsig := ctime
			end
		  end
		else
		  begin
		    restart := false;
		    error_msg := Printf.sprintf "vhdd died with signal %d: not restarting (watchdog never restarts on this signal)" i;
		    exit_code := 12
		  end
	    | Unix.WSTOPPED i ->
		loginfo "receive stop code %i" i;
		Unix.sleep 1;
		(* well, just resume the stop process. the watchdog
		   cannot do anything if the process is stop *)
		Unix.kill !pid Sys.sigcont;
	  with
	    Unix.Unix_error(Unix.EINTR,_,_) -> ()
	  | e -> loginfo "Watchdog received unexpected exception: %s" (Printexc.to_string e)
	end;
      done;
      if !error_msg <> "" then
	begin
	  loginfo "vhdd watchdog exiting.";
	  loginfo "Fatal: %s" !error_msg;
	  Printf.eprintf "%s\n" !error_msg;
	end;
      exit !exit_code    
    end



let _ =
	Random.self_init ();
	let args = [
		"-dummy",Arg.Set Global.dummy,"Sets dummy mode";
		"-dummydir",Arg.Set_string Global.dummydir,"Sets the directory in which to store the dummy data";
		"-host_uuid",Arg.String (fun s -> Global.host_uuid := Some s),"Sets the host uuid (used in dummy mode)";
		"-nodaemon",Arg.Set Global.nodaemon,"Don't daemonize";
		"-port",Arg.Set_int Global.port,"Set port number (default: 4094)";
		"-pidfile",Arg.Set_string Global.pidfile, "Set the pidfile";
		"-fileserver",Arg.Set Global.enable_fileserver,"Enable the fileserver";
		"-htdocs",Arg.Set_string Global.fileserver_base, "Set the base dir for the fileserver";
		"-t",Arg.Set Tracelog.enabled, "Enable the tracelog (warning: unbounded!)";
	] in
	Arg.parse args (fun s -> ()) "Usage: Read the source!";

	Vhd.set_log_level 10;


	if !Global.dummy then begin
		Global.mgt_iface := Some "lo";
		Global.pool_secret := Some "dummy";
		Global.unsafe_mode := true;
(*		Global.nowatchdog := true;*)
(*		Logs.reset_all [ Printf.sprintf "file:%s/var/log/vhdd.log" (Global.get_host_local_dummydir ()) ];*)
		Lvm.Vg.set_dummy_mode (Printf.sprintf "%s" !Global.dummydir) (Printf.sprintf "%s/dev/mapper" (Global.get_host_uuid ())) false;
		Tapdisk.my_context := (Tapctl.create_dummy (Global.get_host_local_dummydir ()));
			
	end;

	Lvm.Lvmdebug.debug_hook := Some MLVMDebug.debug;


	Vhdrpc.local_rpc := Int_server.local_rpc;

	if not !Global.nodaemon then (Unixext.daemonize ()) else Debug.log_to_stdout ();


	(if not !Global.dummy then 
	  Unixext.pidfile_write !Global.pidfile
	else 
	  try Unixext.pidfile_write !Global.pidfile with _ -> ());

        (if not !Global.dummy then
	  Unixext.mkdir_rec "/var/run/sr-mount" 0o755);

	Sys.set_signal Sys.sigpipe Sys.Signal_ignore;

	watchdog server_init
