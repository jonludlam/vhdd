type xapi_generic_params = {
  xgp_localhost_ref : API.ref_host;
  xgp_session_ref : API.ref_session;
  xgp_subtask_of : API.ref_task;
  xgp_sr_ref : API.ref_SR option;
  xgp_vdi_ref : API.ref_VDI option;
}
  
type generic_params = {
  gp_device_config : (string * string) list;
  gp_sr_sm_config : (string * string) list;
  gp_xapi_params : xapi_generic_params option;
}
    
type sr = { 
  sr_uuid : string;
} with rpc
    
type vdi = {
  vdi_uuid : string option;
  vdi_location : string;
} with rpc
    
type configuration_info = {
  key : string;
  description : string;
}

type driver_info = {
  di_name : string;
  di_description : string;
  di_vendor : string;
  di_copyright : string;
  di_driver_version : string;
  di_required_api_version : string;
  di_capabilities : string list;
  di_configuration : configuration_info list;
}

type context = {
  c_driver : string;
  c_api_call : string;
  c_task_id : string;
  mutable c_other_info : (string * string) list
} with rpc

module D = Debug.Make(struct let name="smapi_types" end)
open D

exception Unmarshalling_error of string
exception SmapiFault of int32 * string

let myassoc key args =
  try List.assoc key args with Not_found -> raise (Unmarshalling_error key)

let get_generic_params args =
  try
    { gp_device_config = List.map (fun (k,v) -> (k,XMLRPC.From.string v)) (XMLRPC.From.structure (myassoc "device_config" args));
	  gp_sr_sm_config = List.map (fun (k,v) -> (k,XMLRPC.From.string v)) (try XMLRPC.From.structure (myassoc "sr_sm_config" args) with _ -> []);
      gp_xapi_params = 
	try
	  Some {
	    xgp_localhost_ref=Ref.of_string (XMLRPC.From.string (myassoc "host_ref" args));
	    xgp_session_ref=Ref.of_string (XMLRPC.From.string (myassoc "session_ref" args));
	    xgp_subtask_of=Ref.of_string (XMLRPC.From.string (myassoc "subtask_of" args));
	    xgp_sr_ref =(try Some (Ref.of_string (XMLRPC.From.string (myassoc "sr_ref" args))) with _ -> None);
	    xgp_vdi_ref=(try Some (Ref.of_string (XMLRPC.From.string (myassoc "vdi_ref" args))) with _ -> None);
	  } 
	with Unmarshalling_error _ -> 
	  debug "No xapi parameters"; 
	  None }
  with e ->
    log_backtrace ();
    error "Could not get generic parameters. Dump of args:";
    List.iter (fun (k,v) -> error "Key: %s" k ; error "Value: %s" (Xml.to_string v)) args;
    raise e
      
let get_sr args = 
  { sr_uuid = XMLRPC.From.string (myassoc "sr_uuid" args) }
    
let get_vdi args =
  { vdi_uuid=(try Some (XMLRPC.From.string (myassoc "vdi_uuid" args)) with _ -> None);
    vdi_location=XMLRPC.From.string (myassoc "vdi_location" args) }

let get_named_map name args =
  List.map (fun (k,v) -> (k,XMLRPC.From.string v)) (XMLRPC.From.structure (myassoc name args))

let get_sr_sm_config args = 
  get_named_map "sr_sm_config" args

let get_args args =
  XMLRPC.From.array XMLRPC.From.string (myassoc "args" args)

let get_named_string name args =
  XMLRPC.From.string (myassoc name args)

let get_named_boolean name args =
  XMLRPC.From.boolean (myassoc name args)

let get_named_int name args =
  XMLRPC.From.int (myassoc name args)

let get_sm_config args = 
  get_named_map "sm_config" args

let get_vdi_sm_config args = 
  get_named_map "vdi_sm_config" args

let parse_unit (xml: Xml.xml) = XMLRPC.From.nil xml

let parse_vdi (xml: Xml.xml) =
  let pairs = List.map (fun (key, v) -> key, XMLRPC.From.string v)
    (XMLRPC.From.structure xml) in
  {
    vdi_uuid = Some (myassoc "uuid" pairs);
    vdi_location = myassoc "location" pairs}
      

let to_vdi vi =
  let params = match vi.vdi_uuid with
    | Some uuid -> ["uuid",XMLRPC.To.string uuid]
    | None -> [] in
  XMLRPC.To.structure (("location",XMLRPC.To.string vi.vdi_location)::params)

let get_driver_params args = get_named_map "driver_params" args

let marshal_driver_info di =
  XMLRPC.To.structure [
    "name",XMLRPC.To.string di.di_name;
    "description",XMLRPC.To.string di.di_description;
    "vendor",XMLRPC.To.string di.di_vendor;
    "copyright",XMLRPC.To.string di.di_copyright;
    "driver_version",XMLRPC.To.string di.di_driver_version;
    "required_api_version",XMLRPC.To.string di.di_required_api_version;
    "capabilities",XMLRPC.To.array (List.map (fun cap -> XMLRPC.To.string cap) di.di_capabilities);
    "configuration",XMLRPC.To.array 
      (List.map 
	  (fun ci -> XMLRPC.To.structure ["key",XMLRPC.To.string ci.key; 
					   "description",XMLRPC.To.string ci.description]) 
	  di.di_configuration)]

let unmarshal_driver_info (xml: Xml.xml) = 
  let info = XMLRPC.From.structure xml in
  debug "parsed structure";
  (* Parse the standard strings *)
  let name = XMLRPC.From.string (myassoc "name" info) 
  and description = XMLRPC.From.string (myassoc "description" info)
  and vendor = XMLRPC.From.string (myassoc "vendor" info)
  and copyright = XMLRPC.From.string (myassoc "copyright" info)
  and driver_version = XMLRPC.From.string (myassoc "driver_version" info)
  and required_api_version = XMLRPC.From.string (myassoc "required_api_version" info) in
  let strings = XMLRPC.From.array XMLRPC.From.string (myassoc "capabilities" info) in
  let text_capabilities = strings in
  
  (* Parse the driver options *)
  let configuration = 
    List.map (fun kvpairs -> 
      {key=XMLRPC.From.string (myassoc "key" kvpairs);
       description=XMLRPC.From.string (myassoc "description" kvpairs)})
      (XMLRPC.From.array XMLRPC.From.structure (myassoc "configuration" info)) in
  
  { di_name = name;
    di_description = description;
    di_vendor = vendor;
    di_copyright = copyright;
    di_driver_version = driver_version;
    di_required_api_version = required_api_version;
    di_capabilities = text_capabilities;
    di_configuration = configuration;
  }


