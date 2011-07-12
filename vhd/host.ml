(* Host level things *)

(* This module is mostly concerned with refcounting LV attachments.
   This has turned out to be necessary since switching over to libvhd,
   which requires parent vhds to be accessible when making a clone.
   Conceptually, the refcounting of LVs had been happening in the slave
   module whereas clone happens in the master module, and therefore a
   common lower layer was needed to ensure that clone could safely coexist
   with attach/detach.

   If this changes, we can readdress this issue.
*)



open Threadext
open Int_types

module D=Debug.Debugger (struct let name="host" end)
open D

let mutex = Mutex.create ()
let lv_refcounts = Hashtbl.create 10
let pv_ids : (string * ((string * string) list)) list ref = ref []

exception Already_Registered_PV

let add_pv_id_info sr_uuid new_pv_ids =
	Mutex.execute mutex (fun () ->
		debug "add_pv_id_info: Got mutex";
		pv_ids := ((sr_uuid,new_pv_ids) :: !pv_ids));
	debug "add_pv_id_info: Released mutex"

let remove_pv_id_info sr_uuid =
	Mutex.execute mutex (fun () ->
		debug "remove_pv_id_info: Got mutex";
		pv_ids := List.remove_assoc sr_uuid !pv_ids
	);
	debug "remove_pv_id_info: Released mutex"

let get_pv_ids () =
	List.fold_left (fun acc (sr_uuid,pvids) -> pvids @ acc) [] !pv_ids

let attach_lv_inner ty =
	match ty with
		| Mlvm dmn ->
			(* Nb, the last arg to the next function is purely for dummy mode. It's incorrect as written as it will do the Wrong Thing when you've
			   got multiple SRs attached in dummy mode *)
			let (nod,realname) = Lvm.Vg.lv_activate_internal dmn.dmn_dm_name dmn.dmn_mapping.Camldm.m (get_pv_ids ()) false (snd (List.hd (get_pv_ids ()))) in
			nod

let remove_lv_inner ty =
	match ty with
		| Mlvm dmn ->
			Lvm.Vg.lv_deactivate_internal None dmn.dmn_dm_name

let dm_name ty =
	match ty with
		| Mlvm dmn ->
			dmn.dmn_dm_name

let change_lv_inner ty =
	match ty with
		| Mlvm dmn -> Lvm.Vg.lv_change_internal dmn.dmn_dm_name dmn.dmn_mapping.Camldm.m (get_pv_ids ())

let change_lv ty =
	Mutex.execute mutex (fun () ->
		debug "change_lv: Got the mutex";
		change_lv_inner ty);
	debug "change_lv: Released the mutex"

let bump_refcount ty =
	Mutex.execute mutex (fun () ->
		let dm_name = dm_name ty in
		try
			let (cur,oldty) = Hashtbl.find lv_refcounts dm_name in
			assert(ty=oldty);
			debug "bump_refcount: Got the mutex. dm=%s refcount is now: %d" dm_name (cur+1);
			Hashtbl.replace lv_refcounts dm_name ((cur + 1),oldty)
		with _ ->
			Hashtbl.replace lv_refcounts dm_name (1,ty))

let attach_lv ty =
	let result = Mutex.execute mutex (fun () ->
		debug "attach_lv: Got the mutex";
		let dm_name = dm_name ty in
		try
			let (cur,oldty) = Hashtbl.find lv_refcounts dm_name in
			let newty =
				if ty<>oldty
				then (debug "LV info has changed - altering dm tables";
				debug "oldty: %s" (string_of_lv_attach_info_t oldty);
				debug "newty: %s" (string_of_lv_attach_info_t ty);
				change_lv_inner ty; ty)
				else (debug "LV %s already attached" dm_name; ty)
			in
			Hashtbl.replace lv_refcounts dm_name ((cur + 1),newty);
			debug "attach_lv: increasing refcount for dm=%s to %d" dm_name (cur+1);
			let nod = Lvm.Vg.dev_path_of_dm_name dm_name in
			nod
		with Not_found ->
			debug "LV %s not attached: attaching. refcount now 1" dm_name;
			let result = attach_lv_inner ty in
			Hashtbl.replace lv_refcounts dm_name (1,ty);
			result) in
	debug "attach_lv: released the mutex";
	result

let keep_trying fn arg =
	let rec inner n =
		if n=0 then failwith "Retried too many times";
		try
			fn arg
		with e ->
			debug "Caught exception while keep_trying: %s" (Printexc.to_string e);
			Thread.delay 0.2;
			inner (n-1)
	in
	inner 10

let remove_lv ty =
	let dm_name = dm_name ty in
	Mutex.execute mutex (fun () ->
		let (cur,oldty) = Hashtbl.find lv_refcounts dm_name in
		let new_refcount = cur - 1 in
		debug "Remove LV: refcount for dm=%s is now %d" dm_name new_refcount;
		if new_refcount = 0 then begin
			debug "Removing LV=%s" dm_name;
			keep_trying remove_lv_inner ty;
			Hashtbl.remove lv_refcounts dm_name
		end else
			Hashtbl.replace lv_refcounts dm_name (new_refcount,oldty))

let with_active_lv ty f =
	let nod = attach_lv ty in
	Pervasiveext.finally
		(fun () ->
			f nod)
		(fun () ->
			remove_lv ty)
