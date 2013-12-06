(* utils to call out to vhdd helper python script *)

let helper = "/opt/xensource/sm/vhdd_helper.py"

let attach target port iqn scsiid =
  let (json,_) = Forkhelpers.execute_command_get_output helper ["attach"; target; port; iqn; scsiid] in
  let rpc = Jsonrpc.of_string json in
  match rpc with
  | Rpc.String path -> path
  | _ -> failwith "bad response from helper script"

let detach target port iqn scsiid =
  let (json,_) = Forkhelpers.execute_command_get_output helper ["detach"; target; port; iqn; scsiid] in
  ()

type iqns_dyn = (string list) list with rpc
type iqn = {
  portal: string;
  tpgt: string;
  iqn: string;
}

let probe_iqns target port =
  let (json,_) = Forkhelpers.execute_command_get_output helper ["probe_iqns"; target; port] in
  List.map (function | [portal;tpgt;iqn] -> {portal; tpgt; iqn} | _ -> failwith "Invalid result from probe_iqns") (iqns_dyn_of_rpc (Jsonrpc.of_string json))

type probe_possibility_list = Storage_interface.probe_possibility list with rpc

let probe_luns target port iqn =
  let (json,_) = Forkhelpers.execute_command_get_output helper ["probe_luns"; target; port; iqn] in
  probe_possibility_list_of_rpc (Jsonrpc.of_string json)
