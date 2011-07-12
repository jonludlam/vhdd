(* Abstraction of LVM *)
open Listext
open Stringext
open Int_types
open Lvmabs_types

module D = Debug.Debugger(struct let name="lvmabs" end)
open D

exception Rtte of (container_info * location_info)
exception Vhd_not_found of (container_info * location_info)
exception Container_is_not_an_sr of container_info

let metadata_extents = 1L

let string_of_location location =
	(match location with
		| LogicalVolume lv_name -> Printf.sprintf "LVM: %s" lv_name
		| OLV t -> Printf.sprintf "OLVM: %s" (Olvm.get_lv_name t)
		| File filename -> Printf.sprintf "File: %s" filename)
		
let string_of_location_info location_info =
	Printf.sprintf "%s (%s)"
		(string_of_location location_info.location)
		(match location_info.location_type with
			| Vhd -> "VHD"
			| Raw -> "RAW"
			| Metadata -> "Metadata")

let is_lvm context container =
	match container with
		| VolumeGroup _ -> true
		| FileSystem _ -> false
		| OrigLVMVG _ -> true

let maybe_add_pv_ids context sr_uuid container =
	match container with
		| VolumeGroup vg ->
			let pv_ids = List.map (fun pv -> (pv.Lvm.Pv.id, pv.Lvm.Pv.dev)) vg.Lvm.Vg.pvs in
			(try Host.add_pv_id_info sr_uuid pv_ids with _ -> ())
		| FileSystem _ -> ()
		| OrigLVMVG vg ->
			let pv_ids = Olvm.get_pv_ids vg in
			(try Host.add_pv_id_info sr_uuid pv_ids with _ -> ())

let init_lvm context devices =
	debug "init_lvm: Loading Vg from devices list: [%s]" (String.concat ";" devices);
	let vg = Lvm.Vg.load devices in
	debug "init_lvm: Vg loaded:";
	debug "%s" (Lvm.Vg.to_string vg);
	let vg = Lvm.Vg.init_redo_log vg in
	debug "init_lvm: Redo log initialized:";
	debug "%s" (Lvm.Vg.to_string vg);
	let container = VolumeGroup vg in
	container

let init_fs context path =
	FileSystem path

let init_origlvm context sr_uuid devices =
	let vg = Olvm.init sr_uuid devices in
	let container = OrigLVMVG vg in
	container

let shutdown context container =
	match container with
		| VolumeGroup vg ->
			ignore(Lvm.Vg.write vg true)
		| FileSystem path ->
			()
		| OrigLVMVG _ ->
			()

let container_sr_uuid context container =
	match container with
		| VolumeGroup vg ->
			let stem = "VG_XenStorage-" in
			let stemlen = String.length stem in
			if String.startswith stem vg.Lvm.Vg.name then
				String.sub vg.Lvm.Vg.name stemlen (String.length vg.Lvm.Vg.name - stemlen)
			else
				raise (Container_is_not_an_sr container)
		| FileSystem path ->
			let stem = "/var/run/sr-mount/" in
			let stemlen = String.length stem in
			if String.startswith stem path then
				String.sub path stemlen (String.length path - stemlen)
			else
				raise (Container_is_not_an_sr container)
		| OrigLVMVG vg ->
			let stem = "VG_XenStorage-" in
			let stemlen = String.length stem in
			let vg_name = Olvm.get_vg_name vg in
			if String.startswith stem vg_name then
				String.sub vg_name stemlen (String.length vg_name - stemlen)
			else
				raise (Container_is_not_an_sr container)

let location_uuid context location_info =
	let parse_lv_name lv_name =
		if String.startswith "LV-" lv_name
		then String.sub lv_name 3 (String.length lv_name - 3) (* LV- *)
		else String.sub lv_name 4 (String.length lv_name - 4) (* VHD- *)
	in
	if location_info.location_type = Metadata then failwith "Not a VHD";
	match location_info.location with
		| File f ->
			String.sub f 0 (String.length f - 4) (* strip off .vhd *)
		| LogicalVolume lv_name ->
			parse_lv_name lv_name
		| OLV lv ->
			parse_lv_name (Olvm.get_lv_name lv)

let get_lvm_type lv_name =
	if String.startswith "VHD-" lv_name then
		Vhd
	else if String.startswith "LV-" lv_name then
		Raw
	else
		Metadata

let get_file_type filename =
	if String.endswith ".vhd" filename then
		Vhd
	else
		Metadata
			(* File based SRs don't have 'raw' type disks *)

let scan context container f =
	match container with
		| VolumeGroup vg ->
			List.filter_map (fun lv ->
				let ty = get_lvm_type lv.Lvm.Lv.name in
				f {location=LogicalVolume lv.Lvm.Lv.name; location_type=ty;}) vg.Lvm.Vg.lvs
		| OrigLVMVG vg ->
			let lvs = Olvm.get_lvs vg in
			List.iter (fun lv -> debug "Scan: got LV: %s" (Olvm.get_lv_name lv)) lvs;
			List.filter_map (fun lv ->
				let ty = get_lvm_type (Olvm.get_lv_name lv) in
				f {location=OLV lv; location_type=ty;}) lvs
		| FileSystem path ->
			let rec inner acc dh =
				try
					inner ((Unix.readdir dh)::acc) dh
				with End_of_file ->
					acc
			in
			let files = List.filter (fun f -> f<>"." && f<>"..") (Unixext.with_directory path (inner [])) in
			List.filter_map (fun fname ->
				let ty = get_file_type fname in
				f {location=File fname; location_type=ty;}) files

let operate_on container location_info f_lvm f_fs f_olvm =
	match container,location_info.location with
		| VolumeGroup vg, LogicalVolume lv_name ->
			debug "operating on vg: %s lv: %s" vg.Lvm.Vg.name lv_name;
			let lv =
				try
					List.find (fun lv -> lv.Lvm.Lv.name = lv_name) vg.Lvm.Vg.lvs
				with Not_found ->
					raise (Vhd_not_found (container,location_info))
			in
			let ty = get_lvm_type lv_name in
			let dmname = Lvm.Vg.dm_name_of vg lv in
			let path = Lvm.Vg.dev_path_of vg lv in
			f_lvm vg lv dmname path ty
		| FileSystem fspath, File filepath ->
			debug "operating on file: %s/%s" fspath filepath;
			let path = Printf.sprintf "%s/%s" fspath filepath in
			(try ignore(Unix.LargeFile.stat path) with e -> debug "Got exception: %s" (Printexc.to_string e); raise (Vhd_not_found (container,location_info)));
			let ty = get_file_type path in
			f_fs path ty
		| OrigLVMVG vg, OLV lv ->
			debug "Operating on olvm: %s lv: %s" (Olvm.get_vg_name vg) (Olvm.get_lv_name lv);
			let lv_name = Olvm.get_lv_name lv in
			let ty = get_lvm_type lv_name in
			let path = Olvm.get_dm_path vg lv in
			let dm_name = Olvm.get_dm_name vg lv in
			f_olvm vg lv dm_name path ty
		| _ ->
			raise (Rtte (container,location_info))

let path context container location_info = operate_on container location_info (fun _ _ _ path _ -> path) (fun path _ -> path) (fun _ _ _ path _ -> path)

let exists context container location_info =
	match container,location_info.location with
		| VolumeGroup vg, LogicalVolume lv_name ->
			List.exists (fun lv -> lv.Lvm.Lv.name=lv_name) vg.Lvm.Vg.lvs
		| FileSystem fspath, File filepath ->
			(try ignore (Unix.stat (Printf.sprintf "%s/%s" fspath filepath)); true with Unix.Unix_error (Unix.ENOENT,_,_) -> false)
		| OrigLVMVG vg, OLV lv ->
			Olvm.lv_exists vg lv
		| _ -> failwith "Unexpected pairing error"

let with_active_vhd context container location_info use_tmp f =
	operate_on container location_info
		(fun vg lv dm_name path ty ->
			if use_tmp
			then
				Lvm.Vg.with_active_lv vg lv true (f ty) (* Not refcounted *)
			else
				let map = Lvm.Vg.dm_map_of_lv vg lv true in
				Host.with_active_lv (Mlvm {dmn_dm_name=dm_name; dmn_mapping={Camldm.m=map}}) (f ty)) (* Refcounted *)
		(fun path ty ->
			f ty path)
		(fun vg lv dm_name path ty ->
			let map = Olvm.dm_map_of_lv vg lv in
			let real_dm_name = if use_tmp then Uuid.to_string (Uuid.make_uuid ()) else dm_name in
			Host.with_active_lv (Mlvm {dmn_dm_name=real_dm_name; dmn_mapping={Camldm.m=map}}) (f ty))


let get_attach_info context container location_info =
	operate_on container location_info
		(fun vg lv dm_name path ty ->
			let map = Lvm.Vg.dm_map_of_lv vg lv true in
			Some (Mlvm {dmn_dm_name=dm_name; dmn_mapping={Camldm.m=map}}))
		(fun _ _ -> None)
		(fun vg lv dm_name _ _ ->
			let map = Olvm.dm_map_of_lv vg lv in
			Some (Mlvm {dmn_dm_name=dm_name; dmn_mapping={Camldm.m=map}}))

let create_generic israw context container vhduid size =
	let append_lv_create container location_info lv_name =
		let attach_info = get_attach_info context container location_info in
		match attach_info with 
			| None -> ()
			| Some info -> 
				Tracelog.append context (Tracelog.Master_create_lv (lv_name, info)) None
	in
	match container with
		| VolumeGroup vg ->
			let prefix = if israw then "LV-" else "VHD-" in
			let lv_name = prefix^vhduid in
			let vg = Lvm.Vg.create_lv vg lv_name (Int64.div size Lvm.Constants.extent_size) in
			(*debug "Lv created: %s" (Lvm.Vg.to_string vg);*)
			let lv = List.hd vg.Lvm.Vg.lvs in
			let location_info = {location=LogicalVolume lv.Lvm.Lv.name; location_type=if israw then Raw else Vhd} in
			append_lv_create (VolumeGroup vg) location_info lv_name;
			(VolumeGroup vg, location_info)
		| FileSystem fspath ->
			if israw then failwith "Can't create raw file-based vdi";
			let filename = Printf.sprintf "%s.vhd" vhduid in
			let path = Printf.sprintf "%s/%s" fspath filename in
			let fd = Unix.openfile path [Unix.O_CREAT] 0o600 in
			Unix.close fd;
			(FileSystem fspath, {location=File filename; location_type=Vhd})
		| OrigLVMVG vg ->
			let prefix = if israw then "LV-" else "VHD-" in
			let lv_name = prefix^vhduid in
			let (vg,lv) = Olvm.create_lv vg lv_name size in
			let location_info = {location=OLV lv; location_type=if israw then Raw else Vhd} in
			append_lv_create (OrigLVMVG vg) location_info lv_name;
			(OrigLVMVG vg,location_info)

let create_raw = create_generic true
let create = create_generic false

let find_metadata context container name =
	match container with
		| VolumeGroup vg ->
			let lv_name = name in
			if List.exists (fun lv -> lv.Lvm.Lv.name = lv_name) vg.Lvm.Vg.lvs then
				Some (container, {location=LogicalVolume lv_name; location_type=Metadata})
			else None
		| FileSystem fspath ->
			begin
				try
					let path = Printf.sprintf "%s/%s" fspath name in
					ignore(Unix.stat path);
					Some (container, {location=File name; location_type=Metadata})
				with _ ->
					None
			end
		| OrigLVMVG vg ->
			match Olvm.get_lv_by_name vg name with
				| Some lv -> Some (container, {location=OLV lv; location_type=Metadata})
				| _ -> None

exception MetadataExists

let create_metadata context container name use_mgt =
	(* Sanity check - make sure it doesn't exist *)
	(match find_metadata context container name with
		| Some (container,location) ->
			error "Error: Trying to create a metadata location, but it already exists";
			raise MetadataExists
		| None -> ());

	let metadata_size = Int64.to_int (Int64.mul metadata_extents Lvm.Constants.extent_size) in
	let (container,location_info) =
		match container with
			| VolumeGroup vg ->
				let lv_name = name in
				let location_info = {location=LogicalVolume name; location_type=Metadata} in
				let vg = 
					if use_mgt && (List.exists (fun lv -> lv.Lvm.Lv.name = "MGT") vg.Lvm.Vg.lvs) then begin
						Lvm.Vg.rename_lv vg "MGT" name
					end else begin
						Lvm.Vg.create_lv vg lv_name metadata_extents
					end
				in
				(VolumeGroup (Lvm.Vg.write vg true),location_info)
			| FileSystem fspath ->
				begin
					let path = Printf.sprintf "%s/%s" fspath name in
					let fd = Unix.openfile path [Unix.O_CREAT] 0o600 in
					Unix.close fd;
					(container, {location=File name; location_type=Metadata})
				end
			| OrigLVMVG vg ->
				let create () = Olvm.create_lv vg name (Int64.of_int metadata_size) in
				let (vg,lv) =
					if use_mgt then
						match Olvm.get_lv_by_name vg "MGT" with
							| Some lv -> Olvm.rename_lv vg lv name
							| None -> create ()
					else
						create ()
				in
				let location = {location=OLV lv; location_type=Metadata} in
				let container = OrigLVMVG vg in
				(container,location)
	in
	with_active_vhd context container location_info true (fun ty nod ->
		Circ.init nod metadata_size);
	(container,location_info)


let remove context container location_info =
	operate_on container location_info
		(fun vg lv dm_name path ty ->
			Tracelog.append context (Tracelog.Master_remove_lv lv.Lvm.Lv.name) None;
			VolumeGroup (Lvm.Vg.remove_lv vg lv.Lvm.Lv.name))
		(fun path ty ->
			Unix.unlink path;
			container)
		(fun vg lv dm_name path ty ->
			Tracelog.append context (Tracelog.Master_remove_lv (Olvm.get_lv_name lv)) None;
			let vg = Olvm.destroy_lv vg lv in
			OrigLVMVG vg)

let resize context container location_info new_size =
	let append_lv_resize container location_info lv_name =
		let attach_info = get_attach_info context container location_info in
		match attach_info with 
			| None -> ()
			| Some info -> 
				Tracelog.append context (Tracelog.Master_resize_lv (lv_name, info)) None
	in
	operate_on container location_info
		(fun vg lv dm_name path ty ->
			let size_in_extents =
				Int64.div
					(Int64.add new_size Lvm.Constants.extent_size_minus_one)
					Lvm.Constants.extent_size in
			let vg = Lvm.Vg.resize_lv vg lv.Lvm.Lv.name size_in_extents in
			let container = VolumeGroup (Lvm.Vg.write vg true) in
			let lv_name = lv.Lvm.Lv.name in
			append_lv_resize container location_info lv_name;
			container
		)
		(fun path ty -> container)
		(fun vg lv _ _ _ ->
			let vg = Olvm.resize_lv vg lv new_size in
			let container = OrigLVMVG vg in
			append_lv_resize container location_info (Olvm.get_lv_name lv);
			container)

let commit context container =
	match container with
		| VolumeGroup vg ->
			VolumeGroup (Lvm.Vg.write vg true)
		| _ ->
			container

(* Return a tuple of current size * max size option *)
let size context container location_info =
	operate_on container location_info
		(fun vg lv dm_name path ty ->
			let cursize = Int64.mul (Lvm.Lv.size_in_extents lv) (Lvm.Constants.extent_size) in
			(cursize,Some cursize))
		(fun path ty ->
			let stat = Unix.LargeFile.stat path in
			(stat.Unix.LargeFile.st_size, None))
		(fun vg lv _ path ty ->
			let cursize = Olvm.get_lv_size vg lv in
			(cursize,Some cursize))

let replace_if : ('a -> bool) -> 'a -> 'a list -> 'a list =
	fun pred x' xs ->
		List.map (fun x -> if pred x then x' else x) xs

let add_tag context container location_info (tag : Lvm.Tag.t) =
	let tag_name = Lvm.Tag.string_of tag in
	let append lv_name = Tracelog.append context (Tracelog.Master_lv_add_tag (lv_name,tag_name)) None in
	operate_on container location_info
		(fun vg lv dm_name path ty -> 
			append (lv.Lvm.Lv.name);
			VolumeGroup (Lvm.Vg.add_tag_lv vg lv.Lvm.Lv.name tag))
		(fun path ty ->
			failwith "Can't be done!")
		(fun vg lv _ path ty ->
			 let vg : Olvm.vg = Olvm.add_tag vg lv tag in
			 append (Olvm.get_lv_name lv);
			 OrigLVMVG vg)

let remove_tag context container location_info (tag : Lvm.Tag.t) =
	let tag_name = Lvm.Tag.string_of tag in
	let append lv_name = Tracelog.append context (Tracelog.Master_lv_remove_tag (lv_name,tag_name)) None in
	operate_on container location_info
		(fun vg lv dm_name path ty -> 
			append (lv.Lvm.Lv.name);
			VolumeGroup (Lvm.Vg.remove_tag_lv vg lv.Lvm.Lv.name tag))
		(fun path ty ->
			failwith "Can't be done!")
		(fun vg lv _ path ty ->
			let vg : Olvm.vg = Olvm.remove_tag vg lv tag in
			append (Olvm.get_lv_name lv);
			OrigLVMVG vg)
		
let get_hidden_lvs context container =
	let hidden_tag = (Lvm.Tag.of_string "hidden") in
	match container with
		| OrigLVMVG vg ->
			List.map (fun lv ->
				let ty = get_lvm_type (Olvm.get_lv_name lv) in
				{location=OLV lv; location_type=ty}) (Olvm.get_lvs_with_tag vg hidden_tag)
		| VolumeGroup vg ->
			let lvs = vg.Lvm.Vg.lvs in
			let hidden_lvs = List.filter (fun lv -> List.mem hidden_tag lv.Lvm.Lv.tags) lvs in
			List.map (fun lv -> 
				let ty = get_lvm_type lv.Lvm.Lv.name in
				{location=LogicalVolume lv.Lvm.Lv.name; location_type=ty}) hidden_lvs
		| FileSystem _ -> []

let get_sr_sizes context container =
	match container with
		| OrigLVMVG vg ->
			Olvm.get_vg_sizes vg
		| VolumeGroup vg ->
			0L,0L,0L
				(*failwith "Unimplemented"*)
		| FileSystem path ->
			Fsutils.get_total_used_available path

let write context container location str =
    with_active_vhd context container location false (fun ty f ->
		debug "Writing: %s to path: %s" str f;
		assert(ty=Lvmabs_types.Metadata);
		Circ.write f str
	)

let read context container location =
	with_active_vhd context container location false (fun ty f ->
		assert(ty=Lvmabs_types.Metadata);
		Circ.read f)
