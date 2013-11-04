open Vhd_types

module D=Debug.Make(struct let name="driver" end)
open D

type lvm_type =
	| Local
	| Iscsi
	| Fc

type file_type =
	| Ext
	| Nfs
        | FLocal

type sm_type =
	| Lvm of lvm_type
	| File of file_type
	| OldLvm of lvm_type

(* SR type names *)
let lvmorig = "lvm"
let lvmorigiscsi = "lvmoiscsi"
let lvmorigfc = "lvmohba"
let lvmnew = "lvmnew"
let lvmnewiscsi = "lvmnewiscsi"
let lvmnewfc = "lvmnewhba"
let nfs = "nfs"
let ext = "ext"
let iscsi = "ISCSI"
let hba = "HBA"
let local = "local"

(* Parameters *)

type sm_config_param = { key : string; description : string }

let device = "device" (* ext and lvm *)
let device_p = {key=device; description="The block device on which the SR resides."}

let target = "target" (* lvmoiscsi *)
let target_p = {key=target; description="IP address or hostname of the iSCSI target"}

let targetiqn = "targetIQN" (* lvmoiscsi *)
let targetiqn_p = {key=targetiqn; description="The IQN of the target LUN group to be attached"}

let scsiid = "SCSIid" (* lvmoiscsi and lvmohba *)
let scsiid_p = {key=scsiid; description="The scsi_id of the destination LUN"}

let server = "server" (* nfs *)
let server_p = {key=server; description=""}

let serverpath = "serverpath" (* nfs *)
let serverpath_p = {key=serverpath; description=""}

let localpath = "localpath"
let localpath_p = {key=localpath; description=""}

(* Hidden *)
let localiqn = "localIQN" 
let localiqn_p = {key=localiqn; description=""}


let supports_ha driver_name =
	match driver_name with
		| "lvmoiscsi" 
		| "lvmohba" -> true
		| _ -> false
			
let drivers = [
	lvmorig,     OldLvm Local;
	lvmorigiscsi,OldLvm Iscsi;
	lvmorigfc,   OldLvm Fc;
	lvmnew,      Lvm Local;
	lvmnewiscsi, Lvm Iscsi;
	lvmnewfc,    Lvm Fc;
	nfs,         File Nfs;
	ext,         File Ext;
        local,       File FLocal; ]

let driver_config = [
	lvmorig, [device_p];
	lvmnew, [device_p];
	lvmorigiscsi, [target_p;targetiqn_p;scsiid_p];
	lvmnewiscsi, [target_p;targetiqn_p;scsiid_p];
	lvmorigfc, [scsiid_p];
	lvmnewfc, [scsiid_p];
	nfs, [server_p;serverpath_p];
	ext, [device_p];
        local, [localpath_p]]
	
let get_all_driver_names () =
	List.map fst drivers

let get_driver_config driver_name =
	try 
		List.map (fun x -> x.key, x.description) (List.assoc driver_name driver_config)
	with Not_found -> 
		debug "Couldn't find any driver config for driver: %s" driver_name;
		[]

let of_string string =
	List.assoc string drivers

let string_of driver =
	let (driver_name,ty) = List.find (fun (n,t) -> driver=t) drivers in
	driver_name

let of_ctx ctx =
	of_string ctx.Context.c_driver
