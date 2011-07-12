open Threadext

module D=Debug.Debugger (struct let name="vhd_records" end) 
open D

type pointer =
	| PVhd of string (* vhd UID *)
	| PRaw of Lvmabs_types.location_info

and vhd_record = {
	vhduid : string; (* Primary key *)
	parent : pointer option;
	location : Lvmabs_types.location_info;
	size : Vhdutil.size;
	hidden : int;
}

and slist = string list

and other_info = {
	children : string list;
	refcount : int;
} with rpc

type oi_hashtbl = (pointer, other_info) Hashtbl.t

(* It would be nice not to have the following bits *)
type oi_list = (pointer * other_info) list with rpc
let rpc_of_oi_hashtbl h = rpc_of_oi_list (List.rev (Hashtbl.fold (fun k v acc -> (k,v)::acc) h []))
let oi_hashtbl_of_rpc r = 
	let l = oi_list_of_rpc r in
	let h = Hashtbl.create (List.length l) in
	List.iter (fun (k,v) -> Hashtbl.replace h k v) l;
	h
(* But rpc-light is somewhat restrictive *)

type vhd_record_container = {
	hashtbl : (string, vhd_record) Hashtbl.t;
	other_info : oi_hashtbl;
	mutable coalescable_hidden : slist;
	mutable coalescable_not_hidden : slist;
	mutable relinkable : slist;
	mutable unreachable : pointer list;
	lock : Nmutex.t
} with rpc


let string_of_pointer p = Jsonrpc.to_string (rpc_of_pointer p)

let of_vhds vhdlist rawlvs =
	let hashtbl = Hashtbl.create (List.length vhdlist) in

	(* Work out children relationships *)
	let other_info = Hashtbl.create (List.length vhdlist) in
	List.iter (fun vhd -> 
		Hashtbl.add hashtbl vhd.vhduid vhd;
		match vhd.parent with 
			| Some ptr -> 
				let cur = 
					try Hashtbl.find other_info ptr 
					with Not_found -> {refcount=0; children=[]} (* Refcounts will be filled in later *) 
				in
				Hashtbl.replace other_info ptr {cur with children = vhd.vhduid::cur.children}
			| None -> ()) vhdlist;

	(* Work out refcounts *)
	let refcounts = Hashtbl.create (List.length vhdlist) in
	let rec bump_refcounts ptr delta = 
		(try 
			let cur = Hashtbl.find refcounts ptr in
			Hashtbl.replace refcounts ptr (cur + delta);
		with Not_found ->
			Hashtbl.replace refcounts ptr delta);
		match ptr with 
			| PVhd uid ->
				let vhd = Hashtbl.find hashtbl uid in
				begin match vhd.parent with
					| Some ptr -> bump_refcounts ptr delta
					| None -> ()
				end
			| PRaw x -> ()
	in
	List.iter (fun vhd -> bump_refcounts (PVhd vhd.vhduid) (if vhd.hidden=0 then 1 else 0)) vhdlist;
	List.iter (fun (lvptr,hidden) -> bump_refcounts lvptr (if not hidden then 1 else 0)) rawlvs;

	(* Update the refcounts we've just figured out in the other_info hashtbl *)
	Hashtbl.iter (fun k v -> 
		let cur = try Hashtbl.find other_info k with Not_found -> {refcount=0; children=[]} in
		Hashtbl.replace other_info k {cur with refcount=v}) refcounts;
	
	let only_children = Hashtbl.fold (fun k v acc ->
		if List.length v.children = 1 then ((List.hd v.children)::acc) else acc) other_info [] in
	let coalescable_hidden = List.filter (fun k -> (Hashtbl.find hashtbl k).hidden=1) only_children in
	let coalescable_not_hidden = List.filter (fun k -> (Hashtbl.find hashtbl k).hidden=0) only_children in
	let relinkable = Hashtbl.fold (fun k v acc -> if v.hidden=2 then k::acc else acc) hashtbl [] in	
	let unreachable = Hashtbl.fold (fun k v acc -> if v.refcount=0 then k::acc else acc) other_info [] in
	{
		hashtbl = hashtbl;
		other_info = other_info;
		coalescable_hidden = coalescable_hidden;
		coalescable_not_hidden = coalescable_not_hidden;
		unreachable = unreachable;
		relinkable = relinkable;
		lock = Nmutex.create "vhd_hashtbl_lock";
	}

let to_string t =
	Jsonrpc.to_string (rpc_of_vhd_record_container t)

let get_vhd context t vhduid =
	Nmutex.execute context t.lock
		(Printf.sprintf "Getting VHD uid='%s'" vhduid)
		(fun () ->
			Hashtbl.find t.hashtbl vhduid)

let rec update_refcounts t start delta =
	let cur,isnew = try (Hashtbl.find t.other_info start, false) with _ -> ({refcount=0; children=[]},true) in
	let new_refcount = cur.refcount + delta in
	if new_refcount < 0 then failwith "Refcount < 0 !?";
	begin 
		if cur.refcount=0 
		then t.unreachable <- List.filter (fun x -> x <> start) t.unreachable;
		if new_refcount=0 
		then t.unreachable <- start :: t.unreachable
	end;
	Hashtbl.replace t.other_info start {cur with refcount = cur.refcount + delta};
	match start with 
		| PVhd x ->
			let vhd = Hashtbl.find t.hashtbl x in
			(match vhd.parent with 
				| Some ptr -> update_refcounts t ptr delta
				| None -> ())
		| PRaw x -> ()

let fix_after_removing_child t new_children removed_vhd =
	match new_children with 
		| [] -> 
			t.coalescable_hidden <- List.filter (fun x -> x <> removed_vhd.vhduid) t.coalescable_hidden;
			t.coalescable_not_hidden <- List.filter (fun x -> x <> removed_vhd.vhduid) t.coalescable_not_hidden;
		| [x] ->
			begin match (Hashtbl.find t.hashtbl x).hidden with
				| 0 ->
					t.coalescable_not_hidden <- x::t.coalescable_not_hidden
				| 1 -> 
					t.coalescable_hidden <- x::t.coalescable_hidden
				| 2 -> 
					()
			end
		| _ -> 
			()

let fix_after_adding_child t old_children added_vhd =
	match added_vhd.hidden,old_children with 
		| 0, [] -> 
			t.coalescable_not_hidden <- added_vhd.vhduid :: t.coalescable_not_hidden
		| 1, [] -> 
			t.coalescable_hidden <- added_vhd.vhduid :: t.coalescable_hidden
		| _, [x] -> 
			t.coalescable_not_hidden <- List.filter (fun v -> v <> x) t.coalescable_not_hidden;
			t.coalescable_hidden <- List.filter (fun v -> v <> x) t.coalescable_hidden;
		| _ -> 
			()

let add_vhd context t vhduid vhd =
	Nmutex.execute context t.lock
		(Printf.sprintf "Adding VHD uid='%s'" vhduid)
		(fun () -> 
			if vhd.hidden=2 then t.relinkable <- vhd.vhduid :: t.relinkable;
			Hashtbl.replace t.hashtbl vhduid vhd;
			update_refcounts t (PVhd vhduid) (if vhd.hidden=0 then 1 else 0);
			begin match vhd.parent with
				| Some ptr -> 
					let cur = try Hashtbl.find t.other_info ptr with Not_found -> failwith (Printf.sprintf "Can't find loc: %s" (string_of_pointer ptr)) in
					let newchildren = vhd.vhduid::cur.children in
					Hashtbl.replace t.other_info ptr {cur with children=newchildren};
					fix_after_adding_child t cur.children vhd
				| None ->
					()
			end)

let update_vhd_size context t vhduid newsize =
	Nmutex.execute context t.lock
		(Printf.sprintf "Update_vhd_size uid='%s'" vhduid)
		(fun () ->
			let vhd = Hashtbl.find t.hashtbl vhduid in
			let newvhd = {vhd with size=newsize} in
			Hashtbl.replace t.hashtbl vhduid newvhd;
			newvhd)

let update_hidden context t ptr newhidden =
	Nmutex.execute context t.lock
		(Printf.sprintf "Update_hidden ptr='%s' hidden=%d" (string_of_pointer ptr) newhidden)
		(fun () ->
			match ptr with 
				| PVhd vhduid ->
					let vhd = Hashtbl.find t.hashtbl vhduid in
					let newvhd = {vhd with hidden=newhidden} in
					Hashtbl.replace t.hashtbl vhduid newvhd;

					if vhd.hidden=0 then 
						update_refcounts t ptr (-1);

					(* Fix up coalescable lists *)
					begin match newhidden with 
						| 0 -> 
							update_refcounts t ptr 1;
							if List.mem vhduid t.coalescable_hidden then begin
								t.coalescable_hidden <- List.filter (fun x -> x <> vhduid) t.coalescable_hidden;
								t.coalescable_not_hidden <- vhduid :: t.coalescable_not_hidden
							end else
								t.relinkable <- List.filter (fun x -> x <> vhduid) t.relinkable;
						| 1 -> 
							if List.mem vhduid t.coalescable_not_hidden then begin
								t.coalescable_not_hidden <- List.filter (fun x -> x <> vhduid) t.coalescable_not_hidden;
								t.coalescable_hidden <- vhduid :: t.coalescable_hidden
							end else
								t.relinkable <- List.filter (fun x -> x <> vhduid) t.relinkable;
						| 2 -> 
							t.coalescable_not_hidden <- List.filter (fun x -> x <> vhduid) t.coalescable_not_hidden;
							t.coalescable_hidden <- List.filter (fun x -> x <> vhduid) t.coalescable_hidden;
							t.relinkable <- vhduid :: t.relinkable
						| _ -> 
							failwith "Bad hidden value!"
					end		
				| PRaw lv ->
					()
		)


let update_vhd_parent context t vhduid newparent =
	Nmutex.execute context t.lock
		(Printf.sprintf "Update_vhd_parent uid='%s' parent='%s'" vhduid
			(match newparent with Some ptr -> (Printf.sprintf "Some (%s)" (string_of_pointer ptr)) | None -> "None"))
		(fun () ->
			let vhd = Hashtbl.find t.hashtbl vhduid in
			let newvhd = {vhd with parent=newparent} in
			let oi=Hashtbl.find t.other_info (PVhd vhduid) in
			update_refcounts t (PVhd vhduid) (- oi.refcount);
			Hashtbl.replace t.hashtbl vhduid newvhd;
			update_refcounts t (PVhd vhduid) (oi.refcount);

			begin match newparent with
				| Some ptr ->  
					let cur = Hashtbl.find t.other_info ptr in
					let newchildren = vhduid::cur.children in
					Hashtbl.replace t.other_info ptr {cur with children=newchildren};
					fix_after_adding_child t cur.children vhd
				| None ->
					()
			end;

			begin match vhd.parent with 
				| Some ptr ->
					let cur = Hashtbl.find t.other_info ptr in
					let new_children = List.filter (fun v -> v <> vhduid) cur.children in
					Hashtbl.replace t.other_info ptr {cur with children=new_children};
					fix_after_removing_child t new_children vhd
				| None ->
					()
			end;

			

			newvhd)

let remove_vhd context t vhduid =
	Nmutex.execute context t.lock
		(Printf.sprintf "Removing VHD uid='%s'" vhduid)
		(fun () ->
			let vhd = Hashtbl.find t.hashtbl vhduid in
			if vhd.hidden = 0 then failwith "Can't remove non-hidden VHD";
			let other_info = Hashtbl.find t.other_info (PVhd vhduid) in
			if other_info.refcount <> 0 then failwith "Can't remove VHD that has refcount > 0";
			Hashtbl.remove t.hashtbl vhduid;
			t.relinkable <- List.filter (fun x -> x <> vhduid) t.relinkable;
			t.coalescable_hidden <- List.filter (fun x -> x <> vhduid) t.coalescable_hidden;
			t.coalescable_not_hidden <- List.filter (fun x -> x <> vhduid) t.coalescable_not_hidden;
			t.unreachable <- List.filter (fun x -> x <> (PVhd vhduid)) t.unreachable;
			begin match vhd.parent with 
				| Some ptr ->
					let cur = Hashtbl.find t.other_info ptr in
					let new_children = List.filter (fun v -> v <> vhduid) cur.children in
					Hashtbl.replace t.other_info ptr {cur with children=new_children};
					fix_after_removing_child t new_children vhd
				| None ->
					()
			end;
			vhd)

let remove_lv context t ptr =
	Nmutex.execute context t.lock 
		(Printf.sprintf "Removing LV ptr='%s'" (string_of_pointer ptr))
		(fun () -> 
			try 
				let oi = Hashtbl.find t.other_info ptr in
				if oi.refcount <> 0 then failwith "Can't remove LV - refcount <> 0";
				Hashtbl.remove t.other_info ptr
			with e -> 
				warn "Caught exception whilst removing LV: %s" (Printexc.to_string e))

let get_vhd_hashtbl_copy context t =
	Nmutex.execute context t.lock
		"Getting a copy of the VHD hashtbl"
		(fun () -> Hashtbl.copy t.hashtbl)

let get_vhd_chain context t vhduid =
	Nmutex.execute context t.lock
		"Getting a copy of a VHD chain"
		(fun () ->
			let rec inner vhduid cur =
				let vhd = Hashtbl.find t.hashtbl vhduid in
				match vhd.parent with
					| Some (PVhd child_uid) -> inner child_uid (vhd::cur)
					| Some (PRaw lv) -> (List.rev (vhd::cur),Some lv)
					| None -> (List.rev (vhd::cur),None)
			in inner vhduid [])

let get_children_from_pointer context t pointer =
	Nmutex.execute context t.lock
		(Printf.sprintf "Finding the children of parent %s" (string_of_pointer pointer))
		(fun () -> let oi = Hashtbl.find t.other_info pointer in List.map (fun l -> (l, Hashtbl.find t.hashtbl l)) oi.children)

let get_all_affected_vhds context t parent_ptr =
	Nmutex.execute context t.lock
		"Getting VHDs whose chains go through the specified VHDs"
		(fun () ->
			let copy = Hashtbl.copy t.hashtbl in
			Hashtbl.fold (fun k v acc ->
				debug "checking vhduid=%s" k;
				let rec inner vhduid =
					debug "vhduid: %s" vhduid;
					let vhd = Hashtbl.find copy vhduid in
					match vhd.parent with
						| Some ptr -> 
							if ptr=parent_ptr 
							then true 
							else begin
								match ptr with
									| PVhd vhduid -> inner vhduid
									| _ -> false
							end
						| None -> false
				in
				if inner k then k::acc else acc) t.hashtbl [])

let get_vhd_records_rpc t =
	Mutex.execute t.lock.Nmutex.m (fun () -> rpc_of_vhd_record_container t)

let get_coalesce_info context t = 
	Nmutex.execute context t.lock "Getting coalesce info" (fun () -> 
		(t.coalescable_not_hidden,t.coalescable_hidden,t.relinkable, t.unreachable))

module Tests = struct
	open Ocamltest

	let ctx = {
		Smapi_types.c_driver = "none";
		c_api_call = "none";
		c_task_id = "none";
		c_other_info = [];
	}

	let loc = { Lvmabs_types.location=Lvmabs_types.File "none"; location_type=Lvmabs_types.Vhd } 
	let s = Vhdutil.get_size_for_new_vhd (Int64.mul 1024L 1024L) 
	let vhd1 = {vhduid="1"; parent=None; location=loc; size=s; hidden=0}
	let vhd2 = {vhd1 with vhduid="2"; hidden=1}
	let vhd3 = {vhd1 with vhduid="3"; parent=Some (PVhd "2")}
	let vhd4 = {vhd3 with vhduid="4"}
	let vhd5 = {vhd3 with vhduid="5"; hidden=1}
	let vhd6 = {vhd1 with vhduid="6"; parent=Some (PVhd "5")}
	let vhd7 = {vhd6 with vhduid="7"}
	
	let assertions h nh r u c =
		let p l = print_endline ("[" ^ (String.concat "; " l) ^ "]") in
		try 
			assert_equal_int (List.length c.coalescable_hidden) h;
			assert_equal_int (List.length c.coalescable_not_hidden) nh;
			assert_equal_int (List.length c.relinkable) r;
			assert_equal_int (List.length c.unreachable) u
		with e -> 
			p c.coalescable_hidden;
			p c.coalescable_not_hidden; 
			p c.relinkable;
			p (List.map string_of_pointer c.unreachable);
			print_endline (to_string c);
			raise e

	let check_both_ways vhd_list check =
		let c = of_vhds vhd_list [] in check c;
		print_endline "First way OK";
		let c = of_vhds [] [] in List.iter (fun vhd -> add_vhd ctx c vhd.vhduid vhd) vhd_list; check c;
		print_endline "Second way OK"

	let single_non_hidden = make_test_case "single_non_hidden"
		"Add a single non-hidden VHD and check for appropriate lists"
		begin fun () -> check_both_ways [vhd1] (assertions 0 0 0 0) end

	let single_hidden = make_test_case "single_hidden"
		"Add a single hidden VHD and check for appropriate lists"
		begin fun () -> check_both_ways [vhd2] (assertions 0 0 0 1) end

	let single_relinkable = make_test_case "single_relinkable"
		"Add a single relinkable VHD and check for appropriate lists"
		begin fun () ->	check_both_ways [{vhd1 with hidden=2}] (assertions 0 0 1 1) end

	let leaf_coalescable = make_test_case "leaf_coalescable"
		"Adds two vhds to make a leaf-coalescable VHD tree"
		begin fun () -> check_both_ways [vhd2; vhd3] (assertions 0 1 0 0)	end

	let std_coalescable = make_test_case "std_coalescable"
		"Adds three vhds to make a coalescable VHD tree"
		begin fun () -> check_both_ways [vhd2; vhd5; vhd6] (assertions 1 1 0 0) end

	let std_coalescable_2 = make_test_case "std_coalescable_2"
		"Adds four vhds to make a coalescable VHD tree"
		begin fun () -> check_both_ways [vhd2; vhd5; vhd6; vhd7] (assertions 1 0 0 0) end

	let not_coalescable = make_test_case "not_coalescable"
		"Adds several vhds, creating a tree that is not coalescable at all"
		begin fun () -> check_both_ways [vhd2; vhd3; vhd4; vhd5; vhd6; vhd7] 
			(fun c -> assertions 0 0 0 0;
				let refcounts = ["2",4; "3",1; "4",1; "5",2; "6",1; "7",1] in
				let check (vhduid,refcount) =
					let oi=Hashtbl.find c.other_info (PVhd vhduid) in
					print_endline (Printf.sprintf "Checking refcount of %s (should be %d)" vhduid refcount);
					assert_equal_int oi.refcount refcount
				in List.iter check refcounts
			) end

	let become_leaf_coalescable = make_test_case "become_leaf_coalescable"
		"Adds three vhds to make a VHD tree, then deletes one to create a coalescable tree"
		begin fun () -> 
			let c = of_vhds [vhd2; vhd3; vhd4] [] in
			assertions 0 0 0 0 c;
			update_hidden ctx c (PVhd "3") 1;
			remove_vhd ctx c "3";
			assertions 0 1 0 0 c;
			assert_equal_string (List.hd c.coalescable_not_hidden) "4";
		end

	let become_leaf_coalescable_2 = make_test_case "become_leaf_coalescable_2"
		"Become leaf coalescable as a result of adding VHDs"
		begin fun () ->
			let c = of_vhds [vhd2] [] in
			assertions 0 0 0 1 c;
			add_vhd ctx c "3" vhd3;
			assertions 0 1 0 0 c;
		end

	let become_coalescable = make_test_case "become_coalescable"
		"Sets up a tree and removes a VHD to make another coalescable"
		begin fun () ->
			check_both_ways [vhd2; vhd3; vhd5; vhd6; vhd7] 
				(fun c -> assertions 0 0 0 0 c; update_hidden ctx c (PVhd "3") 1; remove_vhd ctx c "3"; assertions 1 0 0 0 c)
		end

	let tests = make_module_test_suite "Vhd_records"
		[ single_non_hidden; single_hidden; single_relinkable; leaf_coalescable; std_coalescable; 
		std_coalescable_2; not_coalescable; become_leaf_coalescable; become_leaf_coalescable_2;
		become_coalescable ]
end			
