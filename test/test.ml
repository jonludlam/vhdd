open Smapi_types
open Int_types
open Xstringext

module SC=Smapi_client

let rpc host port path = Xmlrpc_client.do_xml_rpc ~version:"1.0" ~host ~port ~path
let intrpc host port = Int_rpc.wrap_rpc (Vhdrpc.remote_rpc "dummy" host port)

let meg = Int64.mul 1024L 1024L
let gig = Int64.mul meg 1024L




let _ =
  Logs.reset_all [ "file:/tmp/test.log" ;]

  let mlvm = ref false in
  let device = ref "/dev/sda3" in
  let sr_uuid = ref "1" in
  let test = ref "none" in
  
  let set_wait_mode = Int_client.Debug.waiting_mode_set (intrpc "localhost" 4094) in
  let slave_mode = ref false in
  let use_iscsi = ref false in
  let target = ref "10.80.224.21" in
  let targetiqn = ref "iqn.2006-01.com.openfiler:jludlam1" in
  let scsiid = ref "14f504e46494c45006374504750302d4b3541312d36335743" in
  let hosts = ref "localhost" in

  Arg.parse [
    "-m", Arg.Set mlvm, "Use the MLVM library";
    "-t", Arg.Set_string test, "Run a test (coalesce, parallel, locking)";
    "-w", Arg.Bool (set_wait_mode), "Set the waiting mode (true|false)";
    "-d", Arg.Set_string device, "Set the device to use (defaults to /dev/sda3)";
    "-u", Arg.Set_string sr_uuid, "Set the SR uuid (defaults to '1')";
    "-i", Arg.Set use_iscsi, "Use ISCSI rather than a local device (specify -target, -targetiqn and -scsiid too)";
    "-h", Arg.Set_string hosts, "Set the hosts list (comma separated list, first is master, defaults to 'localhost')";
    "-target", Arg.Set_string target, "Set the ISCSI target";
    "-targetiqn", Arg.Set_string targetiqn, "Set the ISCSI target IQN";
    "-scsiid", Arg.Set_string scsiid, "Set the ISCSI SCSI id";
    "-s", Arg.Set (slave_mode), "Run in slave mode (defaults to master)"] 
    (fun _ -> failwith "Invalid argument")
    "Usage:\n\ttest [-m] [-d device] [-s uuid] -t [coalesce|parallel|locking]\n\tvc -w [true|false]\n";
  
  let sr = Some {sr_uuid = !sr_uuid} in
  
(*  let gp = {gp_device_config=[
    "targetIQN","iqn.2006-01.com.openfiler:jludlam1"; 
    "target", "10.80.224.21"; 
    "SCSIid", "14f504e46494c45006374504750302d4b3541312d36335743"; 
    "SRmaster","true"]; gp_xapi_params=None} in*)

  let slave_device_config = 
    if !use_iscsi then
      ["target",!target;
       "targetIQN",!targetiqn;
       "SCSIid",!scsiid]
    else
      ["device",!device]
  in

  let master_device_config =
    ("SRmaster","true")::slave_device_config
  in
       
  let master_gp = {gp_device_config=master_device_config; gp_xapi_params=None; gp_sr_sm_config=[]} in
  let slave_gp = {gp_device_config=slave_device_config; gp_xapi_params=None; gp_sr_sm_config=[]} in
  
  let hosts = String.split ',' !hosts in

  let rpc host xml = rpc host 4094 
    (match !mlvm, !use_iscsi with
      | false, false -> "/lvm"
      | false, true -> "/lvmoiscsi"
      | true, false -> "/lvmnew"
      | true, true -> "/lvmnewiscsi") xml
  in
  let intrpc host xml = intrpc host 4094 xml in

  let slaves_and_rpcs = List.map (fun h -> (h, false, slave_gp, rpc h, intrpc h)) (List.tl hosts) in
  let master_and_rpc = let h = List.hd hosts in (h, true, master_gp, rpc h, intrpc h) in
  
  let all = master_and_rpc :: slaves_and_rpcs in

  let testrpc task_id = Xmlrpc_client.do_xml_rpc ~task_id ~subtask_of:"" ~version:"1.0" ~host:"localhost" ~port:4094 ~path:"/lvmtest" in

  let tests = [
    "coalesce", (fun () -> Coalescetest.coalesce_test all sr);
    "parallel", (fun () -> Paralleltest.paralleltest all sr);
(*    "locking", (fun () -> Lockingtest.lockingtest rpc testrpc intrpc gp sr);*)
	"simpletest", (fun () -> Simpletest.simpletest all sr);
    "none", (fun () -> ())
  ] in
  
  let run_test test =
    let (_,_,master_gp, master_rpc, master_intrpc) = List.hd all in
    SC.SR.create master_rpc master_gp sr 0L;
    List.iter (fun (_, _, gp, rpc, _) -> SC.SR.attach rpc gp sr) all;
    test ();
    List.iter (fun (_, _, gp, rpc, _) -> SC.SR.detach rpc gp sr) all;
  in

  let t =
    try List.assoc !test tests 
    with Not_found ->
      Printf.fprintf stderr "Unknown test: try one of (%s)\n" (String.concat "," (List.map fst tests));
      exit 1
  in
  
  run_test t

    
