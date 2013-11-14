(* Original LVM functions *)
open Stringext
open Listext
open Fun


let vgs = "/usr/sbin/vgs"
let pvs = "/usr/sbin/pvs"
let lvs = "/usr/sbin/lvs"
let lvchange = "/usr/sbin/lvchange"
let pvcreate = "/usr/sbin/pvcreate"
let vgcreate = "/usr/sbin/vgcreate"
let lvdisplay = "/usr/sbin/lvdisplay"
let vgextend = "/usr/sbin/vgextend"
let vgchange = "/usr/sbin/vgchange"
let lvcreate = "/usr/sbin/lvcreate"
let lvremove = "/usr/sbin/lvremove"
let lvresize = "/usr/sbin/lvresize"
let lvrename = "/usr/sbin/lvrename"
let pvdisplay = "/usr/sbin/pvdisplay"

module D=Debug.Make(struct let name="olvm" end)
open D

let sector_size=512L

type seg_info = {
	pv_id : string;
	log_start : int64; (* In extents *)
	start : int64; (* In extents *)
	count : int64; (* In extents *)
}

and pv_info = {
	device : string;
	pe_start : int64; (* In sectors *)
	pe_count : int64;
}

and lv_info = (string * ((seg_info list) * (Lvm.Tag.t list))) 

and vg = {
	vg_name : string;
	config_name : string;
	lvs : lv_info list;
	extent_size : int64; (* in sectors *)
	pv_info : (string * pv_info) list;
	pv_ids : (string * string) list; (* device to pvid map *)
} 

and lv = string

with rpc

let nops = ref 0 
let time = ref 0.0

let execute_command_get_output env cmd args =
	debug "Olvm.execute_command_get_output: %s" (String.concat " " (cmd::args));
	debug "env=[%s]" (String.concat ";" (Array.to_list env));
	nops := !nops + 1;
	let t = Unix.gettimeofday () in
	let (out,err) = Forkhelpers.execute_command_get_output ~env cmd args in
	let t = Unix.gettimeofday () -. t in
	time := !time +. t;
	debug "Cumulative: n=%d time=%f" !nops !time;
	if String.length err > 0 then debug "Got stderr from cmd: %s" err;
	out

let execute_command_get_output_send_stdin env cmd args stdin =
	debug "Olvm.execute_command_get_output_send_stdin: %s [sending %s]" (String.concat " " (cmd::args)) stdin;
	nops := !nops + 1;
	let t = Unix.gettimeofday () in
	let (out,err) = Forkhelpers.execute_command_get_output_send_stdin ~env cmd args stdin in
	let t = Unix.gettimeofday () -. t in
	time := !time +. t;
	if String.length err > 0 then debug "Got stderr from cmd: %s" err;
	out

let devdir dirname =
	Printf.sprintf "%s/dev" dirname

let backupdir dirname =
	"/etc/lvm/backup"

let cachedir dirname =
	Printf.sprintf "%s/cache" dirname

let maybe_remove_lvm_conf dirname =
	try ignore(Forkhelpers.execute_command_get_output "/bin/rm" ["-rf";dirname]) with _ -> ()

let really_write_lvm_conf dirname devices =
	maybe_remove_lvm_conf dirname;
	Unixext.mkdir_rec dirname 0o755;
	Unixext.mkdir_rec (devdir dirname) 0o755;
	Unixext.mkdir_rec (backupdir dirname) 0o755;
	Unixext.mkdir_rec (cachedir dirname) 0o755;

	(*let filter_str = String.concat "," (List.map (fun dev -> Printf.sprintf "a|%s|" dev) devices) in*)

	List.iter (fun device ->
		let dest = Printf.sprintf "%s/%s" (devdir dirname) (Filename.basename device) in
		Unixext.unlink_safe dest;
		Unix.symlink device dest) devices;

	let config = String.concat "\n" [
		"devices {";
		"dir=\"/dev\"";
		(Printf.sprintf "scan=\"%s/dev\"" dirname);
		"preferred_names=[]";
		"filter=[\"a|.*|\"]";
		(Printf.sprintf "cache_dir=\"%s/cache\"" dirname);
		"cache_file_prefix=\"\"";
		"write_cache_state=0";
		"sysfs_scan=0";
		"md_component_detection=0";
		"ignore_suspended_devices=0";
		"}";
		"activation {";
		"missing_stripe_filler=\"/dev/ioerror\"";
		"reserved_stack=256";
		"reserved_memory=8192";
		"process_priority=-18";
		"mirror_region_size=512";
		"readahead=\"auto\"";
		"mirror_log_fault_policy=\"allocate\"";
		"mirror_device_fault_policy=\"remove\"";
		"}";
		"global {";
		"umask=63";
		"test=0";
		"units=\"h\"";
		"activation=1";
		"proc=\"/proc\"";
		"locking_type=1";
		"fallback_to_clustered_locking=1";
		"fallback_to_local_locking=1";
		"locking_dir=\"/var/lock/lvm\"";
		"}";
		"shell {";
		"history_size=100";
		"}";
		"backup {";
		"backup=1";
		"backup_dir=\"/etc/lvm/backup\"";
		"archive=0";
		"archive_dir=\"/etc/lvm/archive\"";
		"retain_min=10";
		"retain_days=30";
		"}";
		"log {";
		"verbose=0";
		"syslog=1";
		"overwrite=0";
		"level=0";
		"indent=1";
		"command_names=0";
		"prefix=\"  \"";
		"}"] in

	Unixext.write_string_to_file (Printf.sprintf "%s/lvm.conf" dirname) config

let get_lvm_config_dir config_name =
	Printf.sprintf "/etc/lvm/%s" config_name

let write_lvm_conf force config_name devices =
	let dirname = get_lvm_config_dir config_name in

	if force
	then really_write_lvm_conf dirname devices;

	try
		ignore(Unix.stat dirname)
	with _ ->
		really_write_lvm_conf dirname devices

let getenv config_name =
	[|(Printf.sprintf "LVM_SYSTEM_DIR=%s" (get_lvm_config_dir config_name))|]


let get_dm_name vg lv_name =
	let vg_name = vg.vg_name in
	let vgname = String.concat "--" (Stringext.String.split '-' vg_name) in
	let lvname = String.concat "--" (Stringext.String.split '-' lv_name) in
	Printf.sprintf "%s-%s" vgname lvname

let get_dm_path vg lv_name =
	Printf.sprintf "/dev/mapper/%s" (get_dm_name vg lv_name)

let get_pv_ids vg =
	List.map (fun (x,y) -> (y,x)) vg.pv_ids

let get_vg_name vg =
	vg.vg_name

let get_lvs vg =
	List.map fst vg.lvs

let get_lv_name lv = lv

let lv_exists vg lv_name =
	List.exists (fun (lv_name',_) -> lv_name=lv_name') vg.lvs

let get_lvs_with_tag vg tag =
	List.filter_map (fun (lv_name,(_,tags)) -> if (List.mem tag tags) then Some lv_name else None) vg.lvs

let get_lv_segs config_name vg_name pv_ids lv_name =
	let env = getenv config_name in
	let out = execute_command_get_output env lvs [Printf.sprintf "%s/%s" vg_name lv_name; "--segments";"-o";"seg_start_pe,seg_pe_ranges";"--separator";",";"--noheadings"] in
	let segs = List.map
		(fun l ->
			let fields = String.split ',' (String.strip String.isspace l) in
			match fields with
				| [start_logical_extent;pe_location] -> begin
					match String.split ':' pe_location with
						| [pv_dev; range] -> begin
							match String.split '-' range with
								| [from_e; to_e] ->
									let from_e = Int64.of_string from_e in
									let to_e = Int64.of_string to_e in
									{pv_id = List.assoc pv_dev pv_ids;
									log_start = Int64.of_string start_logical_extent;
									start = from_e;
									count = Int64.add 1L (Int64.sub to_e from_e);
									}
								| _ ->
									debug "Couldn't parse range: %s" range;
									failwith "Bad range"
						end
						| _ ->
							debug "Couldn't parse pe_location: %s" pe_location;
							failwith "Bad pe_location"
				end
				| _ ->
					debug "Couldn't parse fields: %s" l;
					failwith "Bad fields"
		) (String.split '\n' (String.strip (function | '\n' -> true | _ -> false) out))
	in
	segs


let init config_name devices =
	let env = getenv config_name in
	write_lvm_conf true config_name devices;

	let extent_size = ref 0L in

	let cmd_out_to_lines out =
		let lines = String.split '\n' out in
		List.filter_map (fun l -> let x = String.strip String.isspace l in if String.length x>0 then Some x else None) lines
	in

	(* Get the VG name *)
	let out = execute_command_get_output env vgs ["-o";"name";"--noheadings"] in
	let vg_name = String.strip String.isspace out in

	(* Get the PVIDs (device -> pv_id map) *)
	let out = execute_command_get_output env pvdisplay ["-c"] in
	let lines = cmd_out_to_lines out in
	let pv_ids = List.map
		(fun l ->
			let fields = String.split ':' l in
			(List.nth fields 0, List.nth fields 11)) lines in

	List.iter (fun (dev,pv_id) -> debug "pvids: %s,%s" dev pv_id) pv_ids;

	(* Obtain the position of the first physical extent and the size of the physical extents *)
	let out = execute_command_get_output env pvs ["-o"; "+pe_start,pv_pe_count"; "--noheadings"; "--units"; "b"; "--nosuffix"] in
	let lines = cmd_out_to_lines out in
	let pv_info = List.map (fun line ->
		match (String.split_f String.isspace line) with
			| [dev;_;_;_;pe_size_s;_;pe_start_s;pe_count_s] ->
				let pe_size = Int64.of_string pe_size_s in
				let pe_start_bytes = Int64.of_string pe_start_s in
				let pe_start_sectors = Int64.div pe_start_bytes sector_size in
				let pe_count = Int64.of_string pe_count_s in
				let extent_size_bytes = Int64.div pe_size pe_count in
				let extent_size_sectors = Int64.div extent_size_bytes sector_size in
				extent_size := extent_size_sectors;
				(List.assoc dev pv_ids,
				{device=dev;
				pe_start=pe_start_sectors;
				pe_count=pe_count;
				})
			| _ ->
				debug "Couldn't parse pvs output line: %s" line;
				failwith "Couldn't parse pvs output") lines
	in



	(* Get the LVs *)
	let out = execute_command_get_output env lvs ["-o";"lv_name,tags";"--noheadings"] in
	let lv_names_and_tags_strings = List.map 
		(fun str ->
			match String.split_f String.isspace str with
				| [x;y] -> (x,y) 
				| [x] -> (x,"") 
				| (x::_) -> 
					debug "Warning - couldn't quite parse: '%s'" str;
					(x,"")
				| _ -> 
					error "Couldn't parse at all: '%s'" str;
					failwith "Unexpected error"
		)
		(cmd_out_to_lines out) 
	in
	let lv_names_and_tags = List.map (fun (x,y) ->
		(x,List.map Lvm.Tag.of_string $ List.filter (fun s -> String.length s > 0) (String.split ',' y))) lv_names_and_tags_strings in

	let lvs = List.map (fun (lv_name,tags) ->
		let segs = get_lv_segs config_name vg_name pv_ids lv_name in
		(lv_name,(segs,tags))) lv_names_and_tags
	in

	let vg : vg = {vg_name=vg_name;
	config_name = config_name;
	lvs = lvs;
	pv_info=pv_info;
	extent_size = !extent_size;
	pv_ids=pv_ids} in

	let rpc = rpc_of_vg vg in
	debug "VG=%s" (Jsonrpc.to_string rpc);

	vg


let dm_map_of_segments vg segments =
	let extent_to_phys_sector pv extent = Int64.add pv.pe_start (Int64.mul extent vg.extent_size) in
	let extent_to_sector extent = (Int64.mul vg.extent_size extent) in

	let construct_dm_map seg =
		let start = extent_to_sector seg.log_start in
		let len = extent_to_sector seg.count in
		{ Camldm.start=start;
		len = len;
		map = Camldm.Linear { Camldm.device = Camldm.Dereferenced seg.pv_id;
		offset = extent_to_phys_sector (List.assoc seg.pv_id vg.pv_info) seg.start } }
	in

	Array.of_list (List.map construct_dm_map segments)

let dm_map_of_lv vg lv_name =
	let segs = fst (List.assoc lv_name vg.lvs) in
	dm_map_of_segments vg segs

let activate_lv vg lv_name =
	let vg_name = vg.vg_name in
	let env = getenv vg.config_name in
	let _ = execute_command_get_output env lvchange ["-ay";(Printf.sprintf "%s/%s" vg_name lv_name)] in
	()

let deactivate_lv vg lv_name =
	let vg_name = vg.vg_name in
	let env = getenv vg.config_name in
	let _ = execute_command_get_output env lvchange ["-an";(Printf.sprintf "%s/%s" vg_name lv_name)] in
	()

let change_lv vg lv_name =
	let vg_name = vg.vg_name in
	let env = getenv vg.config_name in
	let _ = execute_command_get_output env lvchange ["--refresh";(Printf.sprintf "%s/%s" vg_name lv_name)] in
	()

let add_tag vg lv_name (tag:Lvm.Tag.t) : vg =
	let env = getenv vg.config_name in
	let (segs,tags) = List.assoc lv_name vg.lvs in
	let vg_name = vg.vg_name in
	let _ = execute_command_get_output env lvchange [(Printf.sprintf "%s/%s" vg_name lv_name); "--addtag"; Lvm.Tag.string_of tag] in
	{vg with lvs = (lv_name,(segs,tag::(List.filter (fun t -> t <> tag) tags)))::(List.remove_assoc lv_name vg.lvs)}

let remove_tag vg lv_name (tag:Lvm.Tag.t) : vg =
	let env = getenv vg.config_name in
	let (segs,tags) = List.assoc lv_name vg.lvs in
	let vg_name = vg.vg_name in
	let _ = execute_command_get_output env lvchange [(Printf.sprintf "%s/%s" vg_name lv_name); "--deltag"; Lvm.Tag.string_of tag] in
	{vg with lvs = (lv_name,(segs,(List.filter (fun t -> t <> tag) tags)))::(List.remove_assoc lv_name vg.lvs)}

let create_vg config_name vg_name devices =
	let env = getenv config_name in
	write_lvm_conf true config_name devices;

	let do_device dev =
		(* First destroy anything already on the device *)
		begin
			try
				let _ = Forkhelpers.execute_command_get_output "/bin/dd" ["if=/dev/zero";(Printf.sprintf "of=%s" dev);"bs=512";"count=4";"oflag=direct"] in
				()
			with _ ->
				let _ = Forkhelpers.execute_command_get_output "/bin/dd" ["if=/dev/zero";(Printf.sprintf "of=%s" dev);"bs=512";"count=4"] in
				()
		end;

		ignore(execute_command_get_output env pvcreate ["--metadatasize";"10M";dev])
	in
	List.iter do_device devices;

	(* Create the VG on the first device *)
	ignore(execute_command_get_output env vgcreate [vg_name; List.hd devices]);
	List.iter (fun dev -> ignore(execute_command_get_output env vgextend [vg_name; dev])) (List.tl devices);
	ignore(execute_command_get_output env vgchange ["-an";"--master";vg_name])
		
let get_lv_by_name vg lv_name =
	if lv_exists vg lv_name 
	then Some lv_name
	else None

let create_lv vg lv_name bytes =
	let vg_name = vg.vg_name in
	let env = getenv vg.config_name in
	let size_args = 
		let size_mb = Int64.to_string (Int64.div (Int64.add 1048575L bytes) (1048576L)) in
		["-L";size_mb]
	in
	let _ = execute_command_get_output env lvcreate (size_args @ ["-n";lv_name; vg_name; "--inactive";"-Z";"n"]) in
	let segs = get_lv_segs vg.config_name vg_name vg.pv_ids lv_name in
	({vg with lvs = (lv_name,(segs,[]))::vg.lvs},lv_name)

let destroy_lv vg lv_name =
	let vg_name = vg.vg_name in
	let env = getenv vg.config_name in
	ignore(execute_command_get_output env lvremove ["-f";Printf.sprintf "%s/%s" vg_name lv_name]);
	{vg with lvs = List.remove_assoc lv_name vg.lvs}

let rename_lv vg old_lv_name new_lv_name =
	let vg_name = vg.vg_name in
	let lv = List.assoc old_lv_name vg.lvs in
	let env = getenv vg.config_name in
	ignore(execute_command_get_output env lvrename [vg_name; old_lv_name; new_lv_name]);
	({vg with lvs = (new_lv_name,lv)::(List.remove_assoc old_lv_name vg.lvs)},new_lv_name)

let resize_lv vg lv_name size =
	let vg_name = vg.vg_name in
	let env = getenv vg.config_name in
	let size_mb = Int64.div (Int64.add 1048575L size) (1048576L) in
	(* Check it's not already the correct size *)
	let out = execute_command_get_output env lvdisplay [Printf.sprintf "%s/%s" vg_name lv_name;
	"-C";"-o";"size";"--noheadings";"--units";"m"] in
	(* Returns something of the form: "   40.00M\n" *)
	let cur_mb = String.sub out 0 (String.index out '.') in
	let cur_mb = Stringext.String.strip Stringext.String.isspace cur_mb in
	let cur_mb = Int64.of_string cur_mb in

	let size_mb_rounded = Int64.mul 4L (Int64.div (Int64.add size_mb 3L) 4L) in
	debug "resize_lv: size_mb_rounded: %Ld cur_mb: %Ld" size_mb_rounded cur_mb;
	(if cur_mb <> size_mb_rounded then
		let _ = execute_command_get_output_send_stdin env lvresize [(Printf.sprintf "%s/%s" vg_name lv_name);"-L";(Int64.to_string size_mb);"--ignoredm"] "y\n" in
		());
	let segs = get_lv_segs vg.config_name vg_name vg.pv_ids lv_name in
	let (old_segs,tags) = List.assoc lv_name vg.lvs in
	{vg with lvs = (lv_name,(segs,tags))::(List.remove_assoc lv_name vg.lvs)}

let get_vg_name_from_device device =
	let probe_uuid = Uuidm.to_string (Uuidm.create Uuidm.(`V4)) in
	write_lvm_conf true probe_uuid [device];
	let env = getenv probe_uuid in
	let out = execute_command_get_output env pvs ["--separator";",";"--noheadings"] in
	let lines = String.split '\n' out in
	let vg_names = List.filter_map (fun line ->
		try
			debug "checking line %s" line;
			let fields = String.split ',' line in
			debug "List.nth fields 1=%s" (List.nth fields 1);
			Some (List.nth fields 1)
		with _ -> None) lines
	in
	match List.length vg_names with
		| 0 -> None
		| 1 -> Some(List.hd vg_names)
		| _ -> error "Found more than 1 VG!? got these: [%s]" (String.concat ";" vg_names);
			failwith "Found more than 1 VG in get_vg_name_from_device"

let get_vg_sizes vg =
	let total_extents = List.fold_left (fun size (pv,info) -> Int64.add info.pe_count size) 0L vg.pv_info in
	let used_extents = List.fold_left (fun size (lv_name,(segs,_)) -> let total_lv_extents = List.fold_left (fun size seg -> Int64.add seg.count size) 0L segs in Int64.add total_lv_extents size) 0L vg.lvs in
	let apply3 f (x,y,z) = (f x, f y, f z) in
	apply3 (Int64.mul (Int64.mul vg.extent_size sector_size)) (total_extents, used_extents, Int64.sub total_extents used_extents)

let get_lv_size vg lv_name =
	let (segs,_) =
		try
			List.assoc lv_name vg.lvs
		with Not_found ->
			failwith "Unknown LV"
	in
	let size_in_extents = List.fold_left (fun acc seg_info -> Int64.add acc seg_info.count) 0L segs in
	Int64.mul (Int64.mul size_in_extents vg.extent_size) sector_size (* extent_size is in sectors *)
