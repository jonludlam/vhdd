open Drivers
open Stringext
open Listext
open Vhd_types

exception MissingParam of string

module D=Debug.Make(struct let name="transport" end)
open D

let safe_assoc name params =
	try List.assoc name params with Not_found -> raise (MissingParam name)

let is_mounted dir =
	try
		let stroot = Unix.stat "/" in
		let stdir = Unix.stat dir in
		stroot.Unix.st_dev <> stdir.Unix.st_dev
	with _ ->
		false

let mount_path sr_uuid = Printf.sprintf "/var/run/sr-mount/%s" sr_uuid

let localext_vg_stem = "XSLocalEXT-"

(** Returns the path to the underlying device - either a FS path or a path to a /dev node *)
let attach ty sr_uuid gp is_create =
	let device_config = gp.gp_device_config in
	match ty with
		| Lvm Local
		| OldLvm Local ->
			begin
				try
					safe_assoc Drivers.device device_config
				with _ ->
					raise (Errors.Missing_device_config Drivers.device)
			end
		| Lvm Iscsi
		| OldLvm Iscsi ->
			let localiqn = Iscsilib.get_current_initiator_name () in
			let gp = { gp with
				gp_device_config=
					(match localiqn with
						| Some l -> [(Drivers.localiqn,l)]
						| _ -> []) @ device_config } in
			let sr = Some sr_uuid in
(*			let _ = Smapi_client.SR.attach (Smapi_client.execrpc Drivers.iscsi) gp sr in
			let scsiid = safe_assoc (Drivers.scsiid) device_config in
			let path = Smapi_client.VDI.attach (Smapi_client.execrpc Drivers.iscsi) gp sr scsiid true in
			path
*)
			failwith "Not implemented"

		| OldLvm Fc
		| Lvm Fc ->
			let sr = Some sr_uuid in
			(*let _ = Smapi_client.SR.attach (Smapi_client.execrpc Drivers.hba) gp sr in
			let scsiid = safe_assoc Drivers.scsiid device_config in
			let path = Smapi_client.VDI.attach (Smapi_client.execrpc Drivers.hba) gp sr scsiid true in
			path*)
			failwith "Not implemented"
                | File FLocal ->
                        let path = safe_assoc Drivers.localpath device_config in
                        let _ = Unix.stat path in
                        let sr_path = Printf.sprintf "%s/%s" path sr_uuid in
                        if is_create then begin
                    		Unix.mkdir sr_path 0o777
                        end;
                        let _ = Unix.stat sr_path in
			let sr_path_real = mount_path sr_uuid in
			debug "About to unlink %s" sr_path_real;
			Unixext.unlink_safe sr_path_real;
			let localpath = "/var/run/sr-mount" in
	       		Unixext.mkdir_rec localpath 0700;
			Unix.symlink sr_path sr_path_real;
                        sr_path_real
 		| File Nfs ->
			if is_create then begin
				(* Temporarily mount the parent directory in the NFS server in order to create the SR *)
				let server = safe_assoc Drivers.server device_config in
				let serverpath = Printf.sprintf "%s" (safe_assoc Drivers.serverpath device_config) in
				let localpath = Printf.sprintf "/var/run/sr-mount/%s" sr_uuid
				in
				let transport = "tcp" in
				ignore(Nfs.mount server serverpath localpath transport);
				try
					Unix.mkdir (Printf.sprintf "%s/%s" localpath sr_uuid) 0o777;
					Nfs.unmount localpath
				with
					| Unix.Unix_error (Unix.EEXIST, _, _) ->
						Nfs.unmount localpath;
						failwith "Exists"
			end;
			let server = safe_assoc Drivers.server device_config in
			let serverpath = Printf.sprintf "%s/%s" (safe_assoc Drivers.serverpath device_config) sr_uuid in
			let localpath = Printf.sprintf "/var/run/sr-mount/%s" sr_uuid in
			let transport = "tcp" in
			Nfs.mount server serverpath localpath transport;
		| File Ext ->
			let device = List.assoc Drivers.device device_config in
			let vg_name = Printf.sprintf "%s%s" localext_vg_stem sr_uuid in
			let lv_name = sr_uuid in
			let (vg,lv) =
				if is_create then begin
					Olvm.create_vg sr_uuid vg_name [device];
					let vg = Olvm.init sr_uuid [device] in
					let (total, used, free) = Olvm.get_vg_sizes vg in
					Olvm.create_lv vg lv_name free
				end else begin
					let vg = Olvm.init sr_uuid [device] in
					match Olvm.get_lv_by_name vg lv_name with
						| Some lv ->
							(vg,lv)
						| None ->
							debug "Failed to find appropriate LV (name=%s in VG with name=%s)"
								lv_name (Olvm.get_vg_name vg);
							failwith "Cannot find LV"
				end
			in

			Olvm.activate_lv vg lv;

			try
				let device = Olvm.get_dm_path vg lv in

				if is_create then begin
					let (out,err) = Forkhelpers.execute_command_get_output "/sbin/mkfs.ext3" ["-F"; device] in
					debug "mkfs.ext3 returned: %s" out;
				end;

				let path = mount_path sr_uuid in
				debug "About to make directory: %s" path;
				Unixext.mkdir_rec path 0700;

				if not (is_mounted path) then
					ignore(Forkhelpers.execute_command_get_output "/bin/mount" ["-t";"ext3"; device; path]);

				path
			with e ->
				Olvm.deactivate_lv vg lv;
				log_backtrace ();
				debug "Caught exception while mounting EXT fs (sr=%s device=%s)" sr_uuid device;
				raise e

let detach ty sr_uuid gp =
	let device_config = gp.gp_device_config in
	match ty with
		| Lvm Local
		| OldLvm Local ->
			()
		| Lvm Iscsi
		| OldLvm Iscsi ->
			let localiqn = Iscsilib.get_current_initiator_name () in
			let gp = { gp with
				gp_device_config=
					(match localiqn with
						| Some l -> [(Drivers.localiqn,l)]
						| _ -> []) @ device_config } in
			let sr = Some sr_uuid in
			let scsiid = safe_assoc Drivers.scsiid device_config in
(*			let _ = Smapi_client.VDI.detach (Smapi_client.execrpc Drivers.iscsi) gp sr scsiid in
			Smapi_client.SR.detach (Smapi_client.execrpc Drivers.iscsi) gp sr*)
			failwith "Unimplemented"
		| Lvm Fc
		| OldLvm Fc ->
			let sr = Some sr_uuid in
			let scsiid = safe_assoc Drivers.scsiid device_config in
(*			let _ = Smapi_client.VDI.detach (Smapi_client.execrpc Drivers.hba) gp sr scsiid in
			Smapi_client.SR.detach (Smapi_client.execrpc Drivers.hba) gp sr*)
			failwith "Unimplemented"
		| File FLocal -> 
		  let sr_mount_path = mount_path sr_uuid in
		  debug "About to unlink %s" sr_mount_path;
		  (try Unixext.unlink_safe sr_mount_path with _ -> ())
		| File Nfs ->
			let localpath = mount_path sr_uuid in
			if is_mounted localpath then
				Nfs.unmount localpath

		| File Ext ->
			let localpath = mount_path sr_uuid in

			if is_mounted localpath then
				ignore(Forkhelpers.execute_command_get_output "/bin/umount" [localpath]);

			let device = List.assoc Drivers.device device_config in
			let vg_name = Printf.sprintf "%s%s" localext_vg_stem sr_uuid in
			let lv_name = sr_uuid in
			let (vg,lv) =
				let vg = Olvm.init sr_uuid [device] in
				match Olvm.get_lv_by_name vg lv_name with
					| Some lv ->
						(vg,lv)
					| None ->
						debug "Failed to find appropriate LV (name=%s in VG with name=%s)"
							lv_name (Olvm.get_vg_name vg);
						failwith "Cannot find LV"
			in

			Olvm.deactivate_lv vg lv


let probe ty gp continuation =
	let device_config = gp.gp_device_config in
	match ty with
		| Lvm Local
		| OldLvm Local ->
			let path = attach ty "" gp false in
			let result = continuation path in
			detach ty "" gp;
			result
		| Lvm Iscsi
		| OldLvm Iscsi ->
			let assoc x = try Some (List.assoc x device_config) with Not_found -> None in
			let sid = assoc Drivers.scsiid in
			let tiqn = assoc Drivers.targetiqn in
			let t = assoc Drivers.target in
			begin
				match (t, tiqn, sid) with
					| (Some target, Some targetiqn, Some scsiid) ->
						(* We have everything required to attach the transport layer -
						   so do so and then call the continuation *)
						let tmp_sr_uuid = (Uuidm.to_string (Uuidm.create Uuidm.(`V4))) in
						let path = attach ty tmp_sr_uuid gp false in
						let result = continuation path in
						detach ty tmp_sr_uuid gp;
						result
					| (Some target, Some targetiqn, _) ->
						let tmp_sr_uuid = (Uuidm.to_string (Uuidm.create Uuidm.(`V4))) in
						let localiqn = Iscsilib.get_current_initiator_name () in
						let gp = { gp with
							gp_device_config=
								(match localiqn with
									| Some l -> [(Drivers.localiqn,l)]
									| _ -> []) @ device_config } in
						let sr = Some tmp_sr_uuid in
(*						let _ = Smapi_client.SR.attach (Smapi_client.execrpc Drivers.iscsi) gp sr in
						let vdis = Pervasiveext.finally
							(fun () ->
								Smapi_client.SR.scan (Smapi_client.execrpc Drivers.iscsi) gp sr)
							(fun () ->
								Smapi_client.SR.detach (Smapi_client.execrpc Drivers.iscsi) gp sr)
						in*)
						failwith "Unimplemented";
(*						debug "Got result from ISCSISR: %s" vdis;
						let xml = Xml.parse_string vdis in
						let vdis = match xml with
							| Xml.Element("sr",_,children) ->
								let vdis = List.filter_map
									(function
										| Xml.Element("vdi",_,children) ->
											let location = List.hd (List.filter_map (function Xml.Element("location",_,[Xml.PCData l]) -> Some l | _ -> None) children) in
											let size = List.hd (List.filter_map (function Xml.Element("size",_,[Xml.PCData l]) -> Some l | _ -> None) children) in
											let lunid = List.hd (List.filter_map (function Xml.Element("LUNid",_,[Xml.PCData l]) -> Some l | _ -> None) children) in
											Some (lunid,location,size)
										| _ ->
											None) children in
								vdis
							| _ -> failwith "Failed to scan the ISCSISR"
						in

						Xml.Element("iscsi-target",[],List.map (fun (lun,scsiid,size) ->
							Xml.Element("LUN",[],
							[Xml.Element("SCSIid",[],[Xml.PCData scsiid]);
							Xml.Element("size",[],[Xml.PCData size]);
							Xml.Element("LUNid",[],[Xml.PCData lun])])) vdis)*)
					| _ ->
(*						let localiqn = Iscsilib.get_current_initiator_name () in
						let gp = { gp with
							gp_device_config=
								(match localiqn with
									| Some l -> [(Drivers.localiqn,l)]
									| _ -> []) @ device_config } in
						let test =
							Smapi_client.SR.probe (Smapi_client.execrpc_get_stderr Drivers.iscsi) gp []
						in
						Xml.parse_string test*)
					  failwith "Unimplemented"
			end
		| Lvm Fc
		| OldLvm Fc ->
			begin
				let assoc x = try Some (List.assoc x device_config) with Not_found -> None in
				let sid = assoc Drivers.scsiid in
				begin
					match (sid) with
						| (Some scsiid) -> begin
							(* We have everything required to attach the transport layer -
							   so do so and then call the continuation *)
							let tmp_sr_uuid = (Uuidm.to_string (Uuidm.create Uuidm.(`V4))) in
							let path = attach ty tmp_sr_uuid gp false in
							let result = continuation path in
							detach ty tmp_sr_uuid gp;
							result
						end
						| None ->
(*							let test =
								Smapi_client.SR.probe (Smapi_client.execrpc_get_stderr Drivers.hba) gp []
							in
							Xml.parse_string test*)
						  failwith "Unimplemented"
				end
			end
		| File Nfs ->
			(* We're probing. Let's try using the device-config to attach first *)
			begin
				try
					let server = safe_assoc Drivers.server device_config in
					let serverpath = safe_assoc Drivers.serverpath device_config in
					let localpath = Filename.temp_file "nfs_probe" "" in
					Unix.unlink localpath;
					let transport = "tcp" in
					ignore(Nfs.mount server serverpath localpath transport);
					let result = continuation localpath in
					Nfs.unmount localpath;
					result
				with e ->
					debug "Error probing: %s" (Printexc.to_string e);
					raise e
			end
		| File Ext ->
			begin
				let nothing = Xml.Element("SRlist",[],[]) in
				try
					let device = safe_assoc Drivers.device device_config in
					match Olvm.get_vg_name_from_device device with
						| Some vg_name ->
							debug "Got vg_name=%s" vg_name;
							let len = String.length localext_vg_stem in
							if String.startswith localext_vg_stem vg_name then begin
								debug "Starts with the stub!";
								let sr_uuid = String.sub vg_name len (String.length vg_name - len) in
								debug "uuid=%s" sr_uuid;
(*								if Uuid.is_uuid sr_uuid then*)
								Xml.Element("SRlist",[],[Xml.Element("SR",[],[Xml.Element("UUID",[],[Xml.PCData sr_uuid])])])
(*								else
									nothing*)
							end else nothing
						| None -> nothing
				with e ->
					log_backtrace ();
					debug "Caught exception while probing: %s" (Printexc.to_string e);
					nothing
			end
		| File FLocal ->
		  continuation (safe_assoc Drivers.localpath device_config)
		    

let delete ty sr_uuid gp path =
	let env = [||] in
	let device_config = gp.gp_device_config in

	let do_file_delete () =
		let rec inner acc dh =
			try
				inner ((Unix.readdir dh)::acc) dh
			with End_of_file ->
				acc
		in
		let files = List.filter (fun f -> f<>"." && f<>"..") (Unixext.with_directory path (inner [])) in
		List.iter (fun file -> Unixext.unlink_safe (path ^ "/" ^ file)) files
	in
	match ty with
		| Lvm _
		| OldLvm _ ->
			ignore(Forkhelpers.execute_command_get_output ~env "/bin/dd" ["if=/dev/zero";(Printf.sprintf "of=%s" path);"bs=512";"count=4";"oflag=direct"]);
			detach ty sr_uuid gp
		| Lvm Iscsi
		| OldLvm Iscsi ->
			ignore(Forkhelpers.execute_command_get_output ~env "/bin/dd" ["if=/dev/zero";(Printf.sprintf "of=%s" path);"bs=512";"count=4";"oflag=direct"]);
			detach ty sr_uuid gp
		| OldLvm Fc
		| Lvm Fc ->
			ignore(Forkhelpers.execute_command_get_output ~env "/bin/dd" ["if=/dev/zero";(Printf.sprintf "of=%s" path);"bs=512";"count=4";"oflag=direct"]);
			detach ty sr_uuid gp
		| File FLocal ->
			do_file_delete ();
		  let path = safe_assoc Drivers.localpath device_config in
		  Unix.rmdir (Printf.sprintf "%s/%s" path sr_uuid);
		  detach ty sr_uuid gp
		| File Ext ->
			do_file_delete ();
			detach ty sr_uuid gp;
		| File Nfs ->
			do_file_delete ();
			detach ty sr_uuid gp;
			let server = safe_assoc Drivers.server device_config in
			let serverpath = Printf.sprintf "%s" (safe_assoc Drivers.serverpath device_config) in
			let localpath = Printf.sprintf "/var/run/sr-mount/%s" sr_uuid in
			let transport = "tcp" in
			ignore(Nfs.mount server serverpath localpath transport);
			try
				Unix.rmdir (Printf.sprintf "%s/%s" localpath sr_uuid);
				Nfs.unmount localpath
			with e ->
				error "Caught error while deleting SR! %s" (Printexc.to_string e);
				raise e
			  
