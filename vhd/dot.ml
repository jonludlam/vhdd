open Threadext
open Vhd_types
open Vhd_records
let shorten str =
	String.sub str 0 (min (String.length str) 8)

let to_string context metadata =
	let b = Buffer.create 1024 in
	Nmutex.execute context metadata.m_id_mapping_lock "Making dot" (fun () ->
		Nmutex.execute context metadata.m_vhd_hashtbl_lock "Making dot" (fun () ->
			Printf.bprintf b "digraph vhdtree {\n\tgraph [size=\"18,6\"];\n";

			let ids = Hashtbl.fold (fun k v acc -> (k,v)::acc) metadata.m_data.m_id_to_leaf_mapping [] in
			let ids = List.sort (fun x y -> compare (fst x) (fst y)) ids in

			
(* 
IDs 
*)
			Printf.bprintf b "\t{\n\t\tnode [shape=record];\n\t\trank = same;\n\t\t";
			List.iter (fun (k,v) -> Printf.bprintf b "\"%s\" [label=\"{ID: %s|C: %s|A: %s}\"]; "
				(shorten k) (shorten k) (Vhd_types.current_ops_to_string context v.current_operations) (Vhd_types.attachments_to_string v)) ids;
			Printf.bprintf b "\n\t}\n";

			let vhd_records = Vhd_records.get_vhd_hashtbl_copy context metadata.m_data.m_vhds in 
			let vhds = Hashtbl.fold (fun k v acc -> (k,v)::acc) vhd_records [] in
			let vhds = List.sort (fun x y -> compare (fst x) (fst y)) vhds in

			(* VHDs *)
			Printf.bprintf b "\t{\n\t\tnode [color=lightblue2, style=filled];\n\t\t";
			List.iter (fun (k,v) -> Printf.bprintf b "\"%s\"; " (shorten k)) vhds;
			Printf.bprintf b "\n\t}\n";

			(* ID to leaf *)
			List.iter (fun (k,v) -> Printf.bprintf b "\t\"%s\" -> \"%s\"\n" (shorten k) (shorten (match v.leaf with | PVhd x -> x | PRaw l -> "LV"))) ids;

			(* VHD tree *)
			List.iter (fun (k,v) -> match v.parent with None -> () | Some (PVhd p) -> Printf.bprintf b "\t\"%s\" -> \"%s\"\n" (shorten k) (shorten p) | _ -> ()) vhds;

			Printf.bprintf b "}\n"));
	Buffer.contents b
