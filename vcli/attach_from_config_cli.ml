let rpc path xml = 
  let open Xmlrpc_client in
      XML_protocol.rpc ~transport:(TCP ("localhost",4094)) ~http:(xmlrpc ~version:"1.0" path) xml

let _ =
	let cmdname = Sys.argv.(0) in
	let name = Filename.basename cmdname in
	let sm = String.sub name 0 (String.length name - 2) in
	let rpc xml = 
		rpc (Printf.sprintf "/%s" sm) xml
	in
	Printf.printf "%s\n" (Smapi_client.VDI.attach_from_config rpc Sys.argv.(1))
