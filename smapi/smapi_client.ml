open Smapi_types
open Pervasiveext

module D=Debug.Debugger(struct let name="smapi_client" end)
open D

let methodResponse xml =
	match xml with
		| Xml.Element("methodResponse", _, [ Xml.Element("params", _, [ Xml.Element("param", _, [ param ]) ]) ]) ->
			XMLRPC.Success [ param ]
		| xml -> XMLRPC.From.methodResponse xml

let process unmarshaller response =
	match response with
		| XMLRPC.Success [ r ] ->
			unmarshaller r
		| _ -> failwith "Bad response!"

let sm_dir = "/opt/xensource/sm"
let cmd_name driver = Printf.sprintf "%s/%sSR" sm_dir driver

let spawn_internal cmdarg =
	try
		Forkhelpers.execute_command_get_output cmdarg.(0) (List.tl (Array.to_list cmdarg))
	with
		| Forkhelpers.Spawn_internal_error(log, output, _) ->
			debug "Spawn internal error: log='%s' output='%s'" log output;
			failwith "Spawn internal error"

let execrpc driver xml =
	let args = [| cmd_name driver; Xml.to_string xml |] in
	Array.iter (fun txt -> debug "'%s'" txt) args;
	let output, stderr = spawn_internal args in
	debug "SM stdout: '%s'; stderr: '%s'" output stderr;
	Xml.parse_string output

let execrpc_get_stderr driver xml =
	let args = [| cmd_name driver; Xml.to_string xml |] in
	Array.iter (fun txt -> debug "'%s'" txt) args;
	let output, stderr = spawn_internal args in
	debug "SM stdout: '%s'; stderr: '%s'" output stderr;
	Xml.Element("methodResponse", [], [ Xml.Element("params", [], [Xml.Element("param", [], [Xml.Element("value",[],[Xml.PCData stderr])])])])

type call = {
	(* All calls are performed by a specific Host with a special Session and device_config *)
	session_ref: API.ref_session option;
	host_ref: API.ref_host option;
	device_config: (string * string) list;

	(* SR probe takes sm config at the SR level *)
	sr_sm_config: (string * string) list option;

	(* Most calls operate within a specific SR *)
	sr_ref: API.ref_SR option;
	sr_uuid: string option;

	(* Snapshot and clone supply a set of driver_params from the user *)
	driver_params: (string * string) list option;

	(* Create and introduce supply an initial sm_config from the user *)
	vdi_sm_config: (string * string) list option;
	(* Introduce supplies a UUID to use *)
	new_uuid: string option;

	(* Calls operating on an existing VDI supply both a reference and location *)
	vdi_ref: API.ref_VDI option;
	vdi_location: string option;
	vdi_uuid: string option;

	(* Reference to the task which performs the call *)
	subtask_of: API.ref_task option;

	cmd: string;
	args: string list;
}

let make_call ?driver_params ?sr_sm_config ?vdi_sm_config ?vdi_location ?new_uuid gp sr cmd args =
	match gp.gp_xapi_params with
		| None ->
			{
				session_ref = None;
				host_ref = None;
				device_config = gp.gp_device_config;
				sr_ref = None ;
				sr_uuid = (match sr with Some sr -> Some sr.Smapi_types.sr_uuid | None -> None);
				driver_params = driver_params;
				sr_sm_config = sr_sm_config;
				vdi_sm_config = vdi_sm_config;
				vdi_ref = None;
				vdi_location = vdi_location;
				vdi_uuid = vdi_location;
				new_uuid = new_uuid;
				subtask_of = None;
				cmd = cmd;
				args = args
			}
		| Some xp ->
			{
				session_ref = Some xp.xgp_session_ref;
				host_ref = Some xp.xgp_localhost_ref;
				device_config = gp.gp_device_config;
				sr_ref = xp.xgp_sr_ref;
				sr_uuid = (match sr with Some sr -> Some sr.Smapi_types.sr_uuid | None -> None);
				driver_params = driver_params;
				sr_sm_config = sr_sm_config;
				vdi_sm_config = vdi_sm_config;
				vdi_ref = xp.xgp_vdi_ref;
				vdi_location = vdi_location;
				vdi_uuid = None;
				new_uuid = new_uuid;
				subtask_of = Some xp.xgp_subtask_of;
				cmd = cmd;
				args = args
			}

let xmlrpc_of_call (call: call) =
	let kvpairs kvpairs =
		XMLRPC.To.structure
			(List.map (fun (k, v) -> k, XMLRPC.To.string v) kvpairs) in

	let common = [ "command", XMLRPC.To.string (call.cmd);
	"args", XMLRPC.To.array (List.map XMLRPC.To.string call.args);
	] in
	let dc = [ "device_config", kvpairs call.device_config ] in
	let sr_sm_config = default [] (may (fun x -> [ "sr_sm_config", kvpairs x ]) call.sr_sm_config) in
	let sr_uuid = default [] (may (fun x -> [ "sr_uuid", XMLRPC.To.string x ]) call.sr_uuid) in
	let vdi_location = default [] (may (fun x -> [ "vdi_location", XMLRPC.To.string x ]) call.vdi_location) in
	let vdi_uuid = match call.vdi_uuid with Some x -> call.vdi_uuid | None -> call.vdi_location in
	let vdi_uuid = default [] (may (fun x -> [ "vdi_uuid", XMLRPC.To.string x ]) vdi_uuid) in
	let new_uuid = default [] (may (fun x -> [ "new_uuid", XMLRPC.To.string x ]) call.new_uuid) in

	let driver_params = default [] (may (fun x -> [ "driver_params", kvpairs x ]) call.driver_params) in
	let vdi_sm_config = default [] (may (fun x -> [ "vdi_sm_config", kvpairs x ]) call.vdi_sm_config) in

	let sr_ref = default [] (may (fun x -> ["sr_ref",XMLRPC.To.string (Ref.string_of x)]) call.sr_ref) in
	let vdi_ref = default [] (may (fun x -> ["vdi_ref", XMLRPC.To.string (Ref.string_of x)]) call.vdi_ref) in
	let host_ref = default [] (may (fun x -> ["host_ref",XMLRPC.To.string (Ref.string_of x)]) call.host_ref) in
	let session_ref = default [] (may (fun x -> ["session_ref",XMLRPC.To.string (Ref.string_of x)]) call.session_ref) in
	let subtask_of = default [] (may (fun x -> ["subtask_of",XMLRPC.To.string (Ref.string_of x)]) call.subtask_of) in

	let all = common @ sr_ref @ vdi_ref @ host_ref @ session_ref @ subtask_of @ dc @ sr_sm_config @ sr_uuid @ vdi_location @ vdi_uuid @ driver_params @ vdi_sm_config @ new_uuid in


	XMLRPC.To.methodCall call.cmd [ XMLRPC.To.structure all ]

exception XmlrpcFailure of string * (string list)
exception XmlrpcFault of int32 * string

let e_to_string e =
	match e with
		| XmlrpcFailure (s,ss) -> Printf.sprintf "XmlrpcFailure: %s [%s]" s (String.concat "," ss)
		| XmlrpcFault (i,s) -> Printf.sprintf "XmlrpcFault: %ld %s" i s
		| e -> Printexc.to_string e

let expect_success x =
	match x with
		| XMLRPC.Success [ result ] -> result
		| XMLRPC.Failure (s,ss) -> raise (XmlrpcFailure (s,ss))
		| XMLRPC.Fault (i,s) -> raise (XmlrpcFault (i,s))
		| _ -> failwith "Unexpected xmlrpc result!"

module SR = struct
	let get_driver_info rpc generic_params =
		let call = make_call generic_params None "sr_get_driver_info" [] in
		let response = rpc (xmlrpc_of_call call) in
		Printf.printf "response: %s" (Xml.to_string response);
		let xml = methodResponse response in
		process unmarshal_driver_info xml

	let create rpc gp sr size =
		let call = make_call gp sr "sr_create" [ Int64.to_string size ] in
		parse_unit (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let delete rpc gp sr =
		let call = make_call gp sr "sr_delete" [] in
		parse_unit (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let attach rpc gp sr =
		let call = make_call gp sr "sr_attach" [] in
		parse_unit (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let detach rpc gp sr =
		let call = make_call gp sr "sr_detach" [] in
		parse_unit (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let content_type rpc gp sr =
		let call = make_call gp sr "sr_content_type" [] in
		XMLRPC.From.string (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let probe rpc gp sr_sm_config =
		let call = make_call ~sr_sm_config gp None "sr_probe" [] in
		XMLRPC.From.string (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let scan rpc gp sr =
		let call = make_call gp sr "sr_scan" [] in
		XMLRPC.From.string (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let update rpc gp sr =
		let call = make_call gp sr "sr_update" [] in
		parse_unit (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

end

module VDI = struct
	let attach rpc gp sr vdi_location writable =
		let call = make_call gp sr ~vdi_location "vdi_attach" [string_of_bool writable] in
		XMLRPC.From.string (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let detach rpc gp sr vdi_location =
		let call = make_call gp sr ~vdi_location "vdi_detach" [] in
		parse_unit (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let activate rpc gp sr vdi_location =
		let call = make_call gp sr ~vdi_location "vdi_activate" [] in
		parse_unit (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let deactivate rpc gp sr vdi_location =
		let call = make_call gp sr ~vdi_location "vdi_deactivate" [] in
		parse_unit (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let generate_config rpc gp sr vdi_location =
		let call = make_call gp sr ~vdi_location "vdi_generate_config" [] in
		XMLRPC.From.string (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let attach_from_config rpc xmlstr =
		let xml = Xml.parse_string xmlstr in
		let result = rpc xml in
		Xml.to_string result

	let create rpc gp sr vdi_sm_config size =
		let call = make_call gp sr ~vdi_sm_config "vdi_create" [ Int64.to_string size ] in
		parse_vdi (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let update rpc gp sr vdi_location =
		let call = make_call gp sr ~vdi_location "vdi_update" [] in
		parse_unit (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let introduce rpc gp sr new_uuid vdi_sm_config vdi_location =
		let call = make_call gp sr ~new_uuid ~vdi_sm_config ~vdi_location "vdi_introduce" [] in
		parse_vdi (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let delete rpc gp sr vdi_location =
		let call = make_call gp sr ~vdi_location "vdi_delete" [] in
		parse_unit (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let snapshot rpc gp sr driver_params vdi_location =
		let call = make_call gp sr ~vdi_location ~driver_params "vdi_snapshot" [] in
		parse_vdi (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let clone rpc gp sr driver_params vdi_location =
		let call = make_call gp sr ~vdi_location ~driver_params "vdi_clone" [] in
		parse_vdi (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let resize rpc gp sr driver_params vdi_location size =
		let call = make_call gp sr ~vdi_location ~driver_params "vdi_resize" [Int64.to_string size] in
		parse_vdi (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

	let resize_online rpc gp sr driver_params vdi_location size =
		let call = make_call gp sr ~vdi_location ~driver_params "vdi_resize_online" [Int64.to_string size] in
		parse_vdi (expect_success (methodResponse (rpc (xmlrpc_of_call call))))

end
