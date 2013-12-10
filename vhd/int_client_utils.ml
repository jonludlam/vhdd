open Vhd_types
open Int_types
open Context

module D=Debug.Make(struct let name="int_client_utils" end)
open D

exception NoMaster

let rec slave_retry_loop context allowed_errors (f : (module Int_client.CLIENT) -> 'a) metadata : 'a =
  try
    match metadata.s_data.s_master with 
    | None -> raise NoMaster
    | Some h ->
      let client = Int_client.get h in
      f client
  with
  | IntError(e,args) as exn ->
    if List.mem e allowed_errors
    then (debug "Ignorning allowed error '%s' in slave_retry_loop" e; raise exn)
    else begin
      log_backtrace ();
      error "Unexpected internal exception in slave_retry_loop: %s,[%s]" e (String.concat "; " args);
      Thread.delay 1.0;
      slave_retry_loop context allowed_errors f metadata
    end
  | NoMaster ->
    error "Can't do anything without a master";
    raise NoMaster
  | Int_client.NoIp ->
    error "Can't do anything without a master IP";
    raise NoMaster
  | e ->
    error "Unexpected error in cleanup: %s" (Printexc.to_string e);
    Thread.delay 1.0;
    slave_retry_loop context allowed_errors f metadata

let rec master_retry_loop context allowed_errors bad_errors f metadata host =
  try
    let attached_hosts = Slave_sr_attachments.get_attached_hosts context metadata in
    if not (List.exists (fun ssa -> ssa.ssa_host.h_uuid=host.h_uuid) attached_hosts) then
      raise (IntError(e_sr_not_attached,[host.h_uuid]));
    let client = Int_client.get host in 
    f client
  with
  | IntError(e,args) as exn ->
    if List.mem e bad_errors then begin
      error "Bad error caught: not continuing";
      raise exn
    end;
    if List.mem e allowed_errors
    then debug "Ignorning allowed error '%s' in master_retry_loop" e
    else begin
      log_backtrace ();
      error "Unexpected internal exception in master_retry_loop: %s,[%s]" e (String.concat "; " args);
      Thread.delay 1.0;
      master_retry_loop context allowed_errors bad_errors f metadata host
    end
  | Int_client.NoIp ->
    error "Can't do anything without the slave's IP";
    raise NoMaster
  | e ->
    error "Unexpected error in master_retry_loop: %s" (Printexc.to_string e);
    Thread.delay 1.0;
    master_retry_loop context allowed_errors bad_errors f metadata host
