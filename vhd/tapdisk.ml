open Stringext
open Listext
open Threadext

module D=Debug.Make(struct let name="tapdisk" end)
open D

let dummy_vhd = "/var/run/vhdd/dummy.vhd"

let my_context = ref (Tapctl.create ())
let ctx () = !my_context

let execute_command_get_output env cmd args =
	match Forkhelpers.with_logfile_fd "execute_command_get_out" (fun out_fd ->
		Forkhelpers.with_logfile_fd "execute_command_get_err" (fun err_fd ->
			let pid = Forkhelpers.safe_close_and_exec ~env None (Some out_fd) (Some err_fd) [] cmd args in
			snd(Forkhelpers.waitpid pid)
		)) with
		| Forkhelpers.Success(out,Forkhelpers.Success(err,(status))) ->
			begin
				match status with
					| Unix.WEXITED 0 -> (out,err)
					| Unix.WEXITED n -> raise (Forkhelpers.Spawn_internal_error(err,out,Unix.WEXITED n))
					| Unix.WSTOPPED n -> raise (Forkhelpers.Spawn_internal_error(err,out,Unix.WSTOPPED n))
					| Unix.WSIGNALED n -> raise (Forkhelpers.Spawn_internal_error(err,out,Unix.WSIGNALED n))
			end
		| Forkhelpers.Success(_,Forkhelpers.Failure(_,exn))
		| Forkhelpers.Failure(_, exn) ->
			raise exn

let t_detach t = Tapctl.detach (ctx ()) t; Tapctl.free (ctx ()) (Tapctl.get_minor t)
let t_pause t =  Tapctl.pause (ctx ()) t
let t_unpause t = Tapctl.unpause (ctx ()) t
let get_paused t = Tapctl.is_paused (ctx ()) t
let get_activated t = Tapctl.is_active (ctx ()) t

let tapdevlink_stem = "tapdevlink_"

let get_vhd_link sr_uuid id leaf =
	let leaf_dir = Filename.dirname leaf in
	let vhdlink = Printf.sprintf "%s/%s_%s_%s" leaf_dir (Global.get_host_uuid ()) sr_uuid id in
	vhdlink

let get_tapdev_link sr_uuid id =
	let tapdevlink = Printf.sprintf "%s/%s%s_%s_%s" (Tapctl.get_devnode_dir (ctx ())) tapdevlink_stem (Global.get_host_uuid ()) sr_uuid id in
	tapdevlink

let parse_tapdev_link link =
	debug "parse_tapdev_link: %s" link;
	let len = String.length tapdevlink_stem in
	if String.startswith tapdevlink_stem link then
		try
			let s = String.sub link len (String.length link - len) in
			debug "s=%s" s;
			match String.split '_' s with
				| [host_uuid; sr_uuid; id] ->
					debug "host_uuid=%s sr_Uuid=%s id=%s" host_uuid sr_uuid id;
					if host_uuid = Global.get_host_uuid () then Some (sr_uuid,id) else None
				| _ -> None
		with _ -> None
	else None

let make_vhd_link sr_uuid id new_leaf =
	let vhdlink = get_vhd_link sr_uuid id new_leaf in
	Unixext.unlink_safe vhdlink;
	Unix.symlink new_leaf vhdlink;
	vhdlink

let make_tapdev_link dev sr_uuid id =
	let tapdevlink = get_tapdev_link sr_uuid id in
	let dest = Tapctl.devnode (ctx ()) (Tapctl.get_minor dev) in
	Unixext.unlink_safe tapdevlink;
	Unix.symlink dest tapdevlink;
	tapdevlink

let remove_vhd_link sr_uuid id leaf =
	let vhdlink = get_vhd_link sr_uuid id leaf in
	Unixext.unlink_safe vhdlink

let remove_tapdev_link dev sr_uuid id =
	let tapdevlink = get_tapdev_link sr_uuid id in
	Unixext.unlink_safe tapdevlink

let attach sr_uuid id =
	let minor = Tapctl.allocate (ctx ()) in
	let tid = Tapctl.spawn (ctx ()) in
	let dev = Tapctl.attach (ctx ()) tid minor in
	let tapdev_link = make_tapdev_link dev sr_uuid id in
	(dev,tapdev_link)

let activate dev sr_uuid id leaf ty =
	let link = make_vhd_link sr_uuid id leaf in
	if not (get_activated dev) then begin
		Tapctl._open (ctx ()) dev link ty
	end else begin
		t_pause dev;
		Tapctl.unpause (ctx ()) dev link ty
	end;
	Tapdisk_listen.register (sr_uuid,id) link

let deactivate dev sr_uuid id leaf =
	Tapdisk_listen.unregister (sr_uuid,id);
	remove_vhd_link sr_uuid id leaf;
	Tapctl.close (ctx ()) dev

let detach dev sr_uuid id =
	remove_tapdev_link dev sr_uuid id;
	t_detach dev

let scan () =
	debug "Tapdisk.scan";
	let tapdev_links =
		let d = Unix.opendir (Tapctl.get_devnode_dir (ctx ())) in
		let rec get_tapdev_links () =
			try
				let f = Unix.readdir d in
				let fullf = Printf.sprintf "%s/%s" (Tapctl.get_devnode_dir (ctx ())) f in
				match parse_tapdev_link f with
					| Some (sr_uuid, id) -> begin
						  try
							  let dev = Unix.readlink fullf in
							  ignore(Unix.stat dev); (* Ensure that the link is there *)
							  let stem = Tapctl.get_tapdevstem (ctx ()) in
							  let stemlen = String.length stem in
							  let tapdev_s = String.sub dev stemlen (String.length dev - stemlen) in

							  (int_of_string tapdev_s, (sr_uuid,id))::(get_tapdev_links ())
						  with _ ->
							  debug "Removing link";
							  Unixext.unlink_safe fullf (* remove it if not *);
							  get_tapdev_links ()
					  end
					| None -> get_tapdev_links ()
			with End_of_file -> []
		in
		let result = get_tapdev_links () in
		Unix.closedir d;
		result
	in
    List.iter (fun (minor,(sr_uuid,id)) -> debug "Got tapdevlink: minor=%d, sr_uuid=%s id=%s" minor sr_uuid id) tapdev_links;

    let blktaps = Tapctl.list (ctx ()) in
		
	List.filter_map (fun (tapdev,state,args) ->
		try
			match args with
			    | Some (ty,link) ->
					  let basename = Filename.basename link in
					  debug "Got tapdev: (%d,%d) - vhd=%s" (Tapctl.get_tapdisk_pid tapdev) (Tapctl.get_minor tapdev) link;
					  (match String.split '_' basename with
						  | [host_uuid;sr_uuid;id] ->
								debug "host_uuid=%s sr_uuid=%s id=%s" host_uuid sr_uuid id;
								let (other_end_sr_uuid,other_end_id) = List.assoc (Tapctl.get_minor tapdev) tapdev_links in
								debug "other end: sr_uuid=%s id=%s" other_end_sr_uuid other_end_id;
								Tapdisk_listen.register (sr_uuid,id) link;
								Some (tapdev,sr_uuid, id, Some link)
						  | _ -> None)
			    | None -> 
					  (* Get rid of lingering tapdisks *)
					  try
						  let (sr_uuid, id) = List.assoc (Tapctl.get_minor tapdev) tapdev_links in
						  debug "Found tapdev via link: sr=%s id=%s" sr_uuid id;
						  Some (tapdev, sr_uuid, id, None)
					  with _ ->
						  Tapctl.detach (ctx ()) tapdev;
						  None
		with e -> 
		  debug "Caught exception scanning tapdisks: %s" (Printexc.to_string e);
		  None) blktaps

