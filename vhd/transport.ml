open Drivers
open Xstringext
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
let attach ty sr_uuid device_config is_create =
  let assoc x = try Some (List.assoc x device_config) with Not_found -> None in
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
    let target = safe_assoc Drivers.target device_config in
    let targetiqn = safe_assoc Drivers.targetiqn device_config in
    let scsiid = safe_assoc Drivers.scsiid device_config in
    Vhdd_helper.attach target "3260" targetiqn scsiid
  | OldLvm Fc
  | Lvm Fc ->
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

let detach ty sr_uuid device_config =
  match ty with
  | Lvm Local
  | OldLvm Local ->
    ()
  | Lvm Iscsi
  | OldLvm Iscsi ->
    let target = safe_assoc Drivers.target device_config in
    let targetiqn = safe_assoc Drivers.targetiqn device_config in
    let scsiid = safe_assoc Drivers.scsiid device_config in
    Vhdd_helper.detach target "3260" targetiqn scsiid
  | Lvm Fc
  | OldLvm Fc ->
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


let probe ty device_config continuation =
  match ty with
  | Lvm Local
  | OldLvm Local ->
    let path = attach ty "" device_config false in
    let result = continuation path in
    detach ty "" device_config;
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
	let path = attach ty tmp_sr_uuid device_config false in
	let result = continuation path in
	detach ty tmp_sr_uuid device_config;
	result
      | (Some target, Some targetiqn, _) ->
	let possibilities = Vhdd_helper.probe_luns target "3260" targetiqn in
	Storage_interface.({probed_srs=[]; probed=possibilities; other_data=None})
      | (Some target, _, _) ->
	let iqns = Vhdd_helper.probe_iqns target "3260" in
	Storage_interface.({
	  probed_srs=[]; 
	  probed=List.map 
	    (fun iqn -> 
	      {new_device_config = ["targetIQN",iqn.Vhdd_helper.iqn]; info=[]}) iqns;
	  other_data=None})
      | _ ->
	failwith "Need a target"
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
	  let path = attach ty tmp_sr_uuid device_config false in
	  let result = continuation path in
	  detach ty tmp_sr_uuid device_config;
	  result
	end
	| None ->
	  failwith "Unimplemented"
      end
    end
  | File Nfs ->
    failwith "Unimplemented"
  | File Ext ->
    failwith "Unimplemented"
  | File FLocal ->
    continuation (safe_assoc Drivers.localpath device_config)
		    

let zero_device dev =
  if !Global.dummy then 
    let dir = Printf.sprintf "%s%s" !Global.dummydir dev in
    ignore(Unix.system (Printf.sprintf "rm -rf %s/*" dir));
  else begin
    let oc = Unix.openfile dev [Unix.O_RDWR] 0o000 in
    let str = String.make 2048 '\000' in
    let rec inner left =
      if left=0 then () else
	let written = Unix.write oc str 0 left in
	inner (left - written)
    in inner 2048
  end

let delete ty sr_uuid device_config path =
  let env = [||] in

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
    zero_device path;
    detach ty sr_uuid device_config
  | Lvm Iscsi
  | OldLvm Iscsi ->
    zero_device path;
    detach ty sr_uuid device_config
  | OldLvm Fc
  | Lvm Fc ->
    zero_device path;
    detach ty sr_uuid device_config
  | File FLocal ->
    do_file_delete ();
    let path = safe_assoc Drivers.localpath device_config in
    Unix.rmdir (Printf.sprintf "%s/%s" path sr_uuid);
    detach ty sr_uuid device_config
  | File Ext ->
    do_file_delete ();
    detach ty sr_uuid device_config;
  | File Nfs ->
    do_file_delete ();
    detach ty sr_uuid device_config;
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
			  
