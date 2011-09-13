(* VHDD *)
open Smapi_types
open Stringext

module D=Debug.Debugger(struct let name="vhdd" end)
open D

module P = Process_xmlrpc.Processor(Vhdsm)

let server = Http_svr.Server.empty 

let read_body req fd =
	let len = match req.Http.Request.content_length with Some x -> x | None -> failwith "Need a content length" in
	Unixext.really_read_string fd (Int64.to_int len)

let xmlrpc_handler req fd =
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

	let xml = Xml.parse_string body in

	(* Extract some info from the XML before we pass it to process *)
	let call,args = XMLRPC.From.methodCall xml in
	let context = {
		c_driver=path; 
		c_api_call=call; 
		c_task_id=(match req.Http.Request.task with Some x -> x | None -> Uuid.to_string (Uuid.make_uuid ())); 
		c_other_info=[]; } in
	Tracelog.append context (Tracelog.SmapiCall {Tracelog.path=path; body=(Xml.to_string xml); call=call}) None;
	Debug.associate_thread_with_task context.c_task_id;
	let result = P.process context xml in
	let str = Xml.to_string result in
	Tracelog.append context (Tracelog.SmapiResult {Tracelog.result=str;}) None;
	Tracelog.dump "/tmp/tracelog";
	debug "Response: %s" str;
	Http_svr.response_str req fd str

let internal_handler req fd =
	req.Http.Request.close <- true;
	debug "Internal handler";
	let body = read_body req fd in
	let call = Int_rpc.intrpc_of_rpc (Jsonrpc.of_string body) in
	debug "Call=%s" body;
	(* Extract some info from the XML before we pass it to process *)
(*	let call,args = XMLRPC.From.methodCall xml in*)
	let context = {c_driver="unknown"; c_api_call=""; c_task_id=(match req.Http.Request.task with Some x -> x | None -> Uuid.to_string (Uuid.make_uuid ())); c_other_info=[] } in
	Debug.associate_thread_with_task context.c_task_id;
	(*Tracelog.append context (Tracelog.InternalCall (body, call)) None;*)
	let result = Int_server.process context call in
	let str = Jsonrpc.to_string (Int_rpc.rpc_of_intrpc_response_wrapper result) in
(*	Tracelog.append context (Tracelog.InternalResult str) None;*)
	debug "Response: %s" str;
	Http_svr.response_str req fd str

let register name =
	if not !Global.dummy then begin
		let unix_socket_path = Smapi.unix_socket_path name in
		Unixext.mkdir_safe (Filename.dirname unix_socket_path) 0o700;
		Unixext.unlink_safe unix_socket_path;
		let domain_sock = Http_svr.bind (Unix.ADDR_UNIX(unix_socket_path)) "unix-rpc" in
		Http_svr.start server domain_sock
	end;
	Http_svr.Server.add_handler server Http.Post (Printf.sprintf "/%s" name) (Http_svr.FdIO xmlrpc_handler)

let server_init () =
	Logs.reset_all [ "file:/var/log/vhdd.log" ];

	if !Global.dummy then begin
		Logs.reset_all [ Printf.sprintf "file:%s/var/log/vhdd.log" (Global.get_host_local_dummydir ()) ];
	end;

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

	Global.ready := true;

	while true do
		Thread.delay 20000.;
	done

module MLVMDebug=Debug.Debugger(struct let name="mlvm" end)
module W=Debug.Debugger(struct let name="watchdog" end)

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
		Logs.reset_all [ Printf.sprintf "file:%s/var/log/vhdd.log" (Global.get_host_local_dummydir ()) ];
		Lvm.Vg.set_dummy_mode (Printf.sprintf "%s" !Global.dummydir) (Printf.sprintf "%s/dev/mapper" (Global.get_host_uuid ())) false;
		Tapdisk.my_context := (Tapctl.create_dummy (Global.get_host_local_dummydir ()));
			
	end;

	Lvm.Debug.debug_hook := Some MLVMDebug.debug;

	Vhdrpc.local_rpc := Int_server.local_rpc;

	if not !Global.nodaemon then (Unixext.daemonize ());

	Unixext.pidfile_write !Global.pidfile;

        Unixext.mkdir_rec "/var/run/sr-mount" 0o755;

	Sys.set_signal Sys.sigpipe Sys.Signal_ignore;

	watchdog server_init
