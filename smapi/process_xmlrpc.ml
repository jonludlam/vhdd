open Smapi_types

module D = Debug.Debugger(struct let name="process_xmlrpc" end)
open D

module X=XMLRPC

module Processor =
	functor (SM : Smapi.SMAPI) ->
		struct
			let process context xml =
				let call,args = XMLRPC.From.methodCall xml in
				let args = try XMLRPC.From.structure (List.hd args) with _ -> [] in
				let response =
					try
						match call with
							| "sr_get_driver_info" ->
								let result = marshal_driver_info (SM.SR.get_driver_info context) in
								XMLRPC.Raw [result]
							| "sr_attach" ->
								let gp = get_generic_params args in
								debug "Got generic params";
								let sr = get_sr args in
								debug "Got sr";
								SM.SR.attach context gp sr;
								XMLRPC.Raw [XMLRPC.To.nil ()]
							| "sr_detach" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								SM.SR.detach context gp sr;
								XMLRPC.Raw [XMLRPC.To.nil ()]
							| "sr_content_type" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let response = XMLRPC.To.string (SM.SR.content_type context gp sr) in
								XMLRPC.Raw [response]
							| "sr_probe" ->
								let gp = get_generic_params args in
								let sr_sm_config = get_sr_sm_config args in
								let response = SM.SR.probe context gp sr_sm_config in
								XMLRPC.Raw [XMLRPC.To.string (Printf.sprintf "<?xml version=\"1.0\" ?>\n%s" (Xml.to_string response))]
							| "sr_scan" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let result = SM.SR.scan context gp sr in
								XMLRPC.Raw [XMLRPC.To.string result]
  							| "sr_update" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								SM.SR.update context gp sr;
								XMLRPC.Raw [XMLRPC.To.nil ()]
							| "sr_create" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let size = Int64.of_string (List.hd (get_args args)) in
								SM.SR.create context gp sr size;
								XMLRPC.Raw [XMLRPC.To.nil ()]
							| "sr_delete" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								SM.SR.delete context gp sr;
								XMLRPC.Raw [XMLRPC.To.nil ()]
							| "vdi_attach" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let vdi = get_vdi args in
								let writable = bool_of_string (List.hd (get_args args)) in
								let result = XMLRPC.To.string (SM.VDI.attach context gp sr vdi writable) in
								XMLRPC.Raw [result]
							| "vdi_detach" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let vdi = get_vdi args in
								SM.VDI.detach context gp sr vdi;
								XMLRPC.Raw [XMLRPC.To.nil ()]
							| "vdi_activate" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let vdi = get_vdi args in
								SM.VDI.activate context gp sr vdi;
								XMLRPC.Raw [XMLRPC.To.nil ()]
							| "vdi_deactivate" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let vdi = get_vdi args in
								SM.VDI.deactivate context gp sr vdi;
								XMLRPC.Raw [XMLRPC.To.nil ()]
							| "vdi_generate_config" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let vdi = get_vdi args in
								let result = XMLRPC.To.string (SM.VDI.generate_config context gp sr vdi) in
								XMLRPC.Raw [result]
							| "vdi_attach_from_config" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let vdi = get_vdi args in
								let config = List.hd (get_args args) in
								let result = XMLRPC.To.string (SM.VDI.attach_from_config context gp sr vdi config) in
								XMLRPC.Raw [result]
							| "vdi_create" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let sm_config = get_vdi_sm_config args in
								let size = Int64.of_string (List.hd (get_args args)) in
								let result = to_vdi (SM.VDI.create context gp sr sm_config size) in
								XMLRPC.Raw [result]
							| "vdi_update" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let vdi = get_vdi args in
								SM.VDI.update context gp sr vdi;
								XMLRPC.Raw [XMLRPC.To.nil ()]
							| "vdi_introduce" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let new_uuid = get_named_string "new_uuid" args in
								let sm_config = get_vdi_sm_config args in
								let location = get_named_string "vdi_location" args in
								let result = to_vdi (SM.VDI.introduce context gp sr new_uuid sm_config location) in
								XMLRPC.Raw [result]
							| "vdi_delete" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let vdi = get_vdi args in
								SM.VDI.delete context gp sr vdi;
								XMLRPC.Raw [XMLRPC.To.nil ()]
							| "vdi_snapshot" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let driver_params = get_driver_params args in
								let vdi = get_vdi args in
								let result = to_vdi (SM.VDI.snapshot context gp driver_params sr vdi) in
								XMLRPC.Raw [result]
							| "vdi_clone" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let driver_params = get_driver_params args in
								let vdi = get_vdi args in
								let result = to_vdi (SM.VDI.clone context gp driver_params sr vdi) in
								XMLRPC.Raw [result]
							| "vdi_resize" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let vdi = get_vdi args in
								let size = Int64.of_string (List.hd (get_args args)) in
								let result = to_vdi (SM.VDI.resize context gp sr vdi size) in
								XMLRPC.Raw [result]
							| "vdi_resize_online" ->
								let gp = get_generic_params args in
								let sr = get_sr args in
								let vdi = get_vdi args in
								let size = Int64.of_string (List.hd (get_args args)) in
								let result = to_vdi (SM.VDI.resize_online context gp sr vdi size) in
								XMLRPC.Raw [result]
							| _ ->
								XMLRPC.Fault(0l,(Printf.sprintf "Unknown API call %s" call))
					with
						| SmapiFault (code,str) ->
							XMLRPC.Fault(code,str)
						| e ->
							log_backtrace ();
							XMLRPC.Fault (0l,"INTERNAL_ERROR: "^(Printexc.to_string e))
				in
				XMLRPC.To.methodResponse response

			let local_rpc xml =
				process xml

			let remote_rpc host port path xml =
			  let open Xmlrpcclient in
			      XML_protocol.rpc ~transport:(TCP (host,port)) ~http:(xmlrpc ~version:"1.0" path) xml

		end
