open Listext
open Vhd_types
open Vhd_records

module D=Debug.Make(struct let name="scan" end)
open D

let scan context container =
	debug "scan";
	let vhd_hashtbl : (string,vhd_record) Hashtbl.t = Hashtbl.create 10 in
	let path_to_location_map = Lvmabs.scan context container (fun location -> Some (Lvmabs.path context container location, location)) in
	let read_vhd location ty path =
		debug "Reading VHD: %s" path;
		match ty with
			|	Lvmabs_types.Vhd ->
				let vhd = Vhd._open path [Vhd.Open_rdonly] in
				let vhduid = Vhd.get_uid vhd in
				let hidden = Vhd.get_hidden vhd in
				let parent =
					try
						let parent_uid = Vhd.get_parent_uid vhd in
						if parent_uid = Vhdutil.zero_uuid
						then begin
							(* Need to find the parent LV name *)
							let rel_parent_locator = Vhd.get_parent vhd in
							debug "Got raw parent locator: %s" rel_parent_locator;
							let parent_fname = Filename.basename rel_parent_locator in
							let parent_dname = Filename.dirname path in (* Take path of VHD, append filename of parent *)
							let parent_locator = Filename.concat parent_dname parent_fname in
							try
								Some (PRaw (List.assoc parent_locator path_to_location_map))
							with
								| e ->
									error "Error locating raw parent for VHD: %s (%s)" vhduid
										(Lvmabs.string_of_location_info location);
									error "Exception: %s" (Printexc.to_string e);
									raise e
						end else begin
							Some (PVhd parent_uid)
						end
					with e ->
						error "Caught exception in getting parent: %s" (Printexc.to_string e);
						None
				in
				let bat = Vhd.get_bat vhd in
				debug "BAT:";
				List.iter (fun (start,len) -> debug "Start: %d Len: %d" start len) bat;
				Vhd.close vhd;
				Some { vhduid=vhduid;
				parent=parent;
				location=location;
				size=Vhdutil.query_size path;
				hidden=hidden;
				}
			| Lvmabs_types.Raw ->
				failwith "Raw LVs Unimplemented!"
			| Lvmabs_types.Metadata ->
				None
	in
	Lvmabs.scan context container
		(fun location ->
			try
				Lvmabs.with_active_vhd context container location true (read_vhd location)
			with e ->
				debug "Got exception when scanning VHD %s: %s" (Lvmabs.string_of_location_info location) (Printexc.to_string e);
				log_backtrace ();
				None)

