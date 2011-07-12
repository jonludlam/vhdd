(* DummyD *)
open Smapi_types
open Stringext

module D=Debug.Debugger(struct let name="dummyd" end)
open D

module P = Process_xmlrpc.Processor(Dummysm)

let xmlrpc_handler req bio =
  let path = match String.split '/' req.Http.uri with
    | x::path::_ -> path
    | _ -> failwith "Unknown path"
  in
  debug "path=%s" path;
  let body = Http_svr.read_body ~limit:(999999999) req bio in
  debug "Request: %s" body;
  let s = Buf_io.fd_of bio in
  let xml = Xml.parse_string body in
  let result = P.process {driver=path} xml in
  let str = Xml.to_string result in
  debug "Response: %s" str;
  Http_svr.response_str req s str

let register name =
  let unix_socket_path = Smapi.unix_socket_path name in
  Unixext.mkdir_safe (Filename.dirname unix_socket_path) 0o700;
  Unixext.unlink_safe unix_socket_path;
  let domain_sock = Http_svr.bind (Unix.ADDR_UNIX(unix_socket_path)) in
  ignore(Http_svr.start (domain_sock, "unix-RPC"));
  Http_svr.add_handler Http.Post (Printf.sprintf "/%s" name) (Http_svr.BufIO xmlrpc_handler)


let daemonize () =
  Unixext.daemonize ();

  register "dummy2";

  let localhost = Unix.inet_addr_of_string "127.0.0.1" in
  let localhost_sock = Http_svr.bind (Unix.ADDR_INET(localhost, 4094)) in
  Unix.setsockopt localhost_sock Unix.SO_REUSEADDR true;
  ignore(Http_svr.start (localhost_sock, "inet-RPC"));

  while true do
    Thread.delay 20000.;
  done

module MLVMDebug=Debug.Debugger(struct let name="mlvm" end)
  
let _ =
  Lvm.Vg.debug_hook := Some MLVMDebug.debug;
  Logs.reset_all [ "file:/var/log/lvhd2.log" ];
(*  Lvhd.local_rpc := Process_xmlrpc.local_rpc;*)
  daemonize ()

