(* HTML Status *)
open Listext
open Pervasiveext
open Vhd_types
open Int_types
open Stringext
open Threadext
open Vhd_records
open Context

type updates = {
	id : int;
	master_metadatas : master_sr_metadata_data list;
	slave_metadatas : slave_sr_metadata_data list;
} with rpc

let tag name ?(attrs=[]) fs () =
	let a = String.concat "" (List.filter_map (fun (a,v) -> may (fun v -> Printf.sprintf " %s=\"%s\"" a v) v) attrs) in
	match fs with
		| [] -> Printf.sprintf "<%s%s></%s>" name a name
		| _ -> Printf.sprintf "<%s%s>%s</%s>" name a (String.concat "" (List.map (fun f -> f ()) fs)) name
let text t () = t
let html = tag "html"
let head = tag "head"
let title = tag "title"
let style = tag "style"
let body = tag "body"
let h n = tag (Printf.sprintf "h%d" n)
let div = tag "div"
let p = tag "p"
let table = tag "table"
let thead = tag "thead"
let oddrow = ref true
let tr ?attrs stuff =
	oddrow := not !oddrow;
	let extra_attrs = ("class",if !oddrow then Some "oddrow" else None) in
	match attrs with
		| Some a -> tag ~attrs:(extra_attrs::a) "tr" stuff
		| None -> tag ~attrs:[extra_attrs] "tr" stuff
let td = tag "td"
let th = tag "th"
let span = tag "span"
let form = tag "form"
let input = tag "input"


let css = "
body {font-family:sans; font-size: xx-small; background:#ddd}
span {
	 font-size:large;
	 font-weight:bold;
}
h1 {clear:both;}
div.attached_as_master {
	 background:#fff;
	 border:1px solid #aaa;
	 padding:10px;
	 float:left;
	 margin-bottom:30px;
}
div.attached_as_slave {
	 background:#fff;
	 border:1px solid #aaa;
	 padding:10px;
	 float:left;
	margin-bottom:30px;
}
div.debug {
	 background:#fff;
	 border:1px solid #aaa;
	 padding:10px;
	 float:left;
	margin-bottom:30px;
}
table {border-collapse: collapse;
border:1px solid #aaa;
font-size:xx-small;
}
td { margin:0px;
	 padding: 2px;
	 padding-right:30px;
	 max-width:250px;
}
thead th {
text-transform:uppercase;
text-align:left;
background:#000;
color:#fff;
padding-right:30px;
}

tr {
	background:#eef;
}

tr.oddrow {
		background:#ddf;
}
"

let ptr_to_text p =
	match p with
		| PVhd x -> Printf.sprintf "(PVhd '%s')" x
		| PRaw l -> Printf.sprintf "(PRaw '%s')" (Lvmabs.string_of_location_info l)

let vhd_to_tr k vhd =
	tr [td [text vhd.vhduid];
	td [text (Int64.to_string (Vhdutil.get_virtual_size vhd.size))];
	td [text (Int64.to_string (Vhdutil.get_phys_size vhd.size))];
	td [text (Int64.to_string (Vhdutil.get_critical_size vhd.size))];
	td [text (match vhd.parent with None -> "N/A" | Some x -> ptr_to_text x)];
	td [text (Lvmabs.string_of_location_info vhd.location)];
	td [text (string_of_int vhd.hidden)]
	]

let ssa_to_tr ssa =
	tr [td [text ssa.ssa_host.h_uuid];
	td [text ssa.ssa_host.h_ip];
	td [text (string_of_bool ssa.ssa_resync_required)]
	]

let leaf_info_to_tr (k,v) =
	tr [td [text k];
	td [text (Vhd_types.attachments_to_string v)];
	td [text (Vhd_types.activations_to_string v)];
	td [text (ptr_to_text v.leaf)]]

let waiting_processes () =
	let rows = List.map (fun (k,v) ->
		tr [td [text k];
		td [text v.lc_context.c_api_call];
		td [text v.lc_context.c_task_id];
		td [text v.lc_reason];
		td [text v.lc_lock_required];
		td [form ~attrs:["action",Some (Printf.sprintf "/unwait/%s" k); "method", Some "POST"] [input ~attrs:["type",Some "submit"; "value",Some "unblock"] []]]
		]) (Nmutex.get_waiting_list ())
	in
	table ([thead [tr [th [text "uuid"]; th [text "api_call"]; th [text "task_id"]; th [text "reason"]; th [text "lock required"]; th [text "unblock"]]]] @ rows)

let master_to_html context k metadata =
	div [span [text "SR UUID: "; text metadata.m_data.m_sr_uuid];

(*	div ~attrs:["class",Some "master_vhds"]
		[h 2 [text "VHDs"];
		table ((thead [tr [th [text "vhduid"]; th [text "virtual_size"]; th [text "phys_size"]; th [text "critical_size"]; th [text "parent"]; th [text "location"]; th [text "hidden"]]]) :: (Hashtbl.fold (fun k v acc -> (vhd_to_tr k v)::acc) metadata.m_data.m_vhds.hashtbl []));
		];*)

	div ~attrs:["class",Some "master_attached_hosts"]
		[h 2 [text "Connected hosts"];
		table ((thead [tr [th [text "uuid"]; th [text "ip"]; th [text "needs resync"]]]) :: (List.map ssa_to_tr metadata.m_data.m_attached_hosts))
		];

(*	div ~attrs:["class",Some "master_leaf_info"]
		[h 2 [text "Leaf info"];
		table ((thead [tr [th [text "id"]; th [text "current operations"]; th [text "attachments"]; th [text "activations"]; th [text "leaf uid"]]]) ::
			(List.map leaf_info_to_tr (Locking.get_all_leaf_infos context metadata)))
		];*)
	]

let attached_vdi_to_table_row location savi =
	tr [td [text location];
	td [text (let x = savi.savi_blktap2_dev in Printf.sprintf "(%d,%d)" (Tapctl.get_minor x) (Tapctl.get_tapdisk_pid x))];
	td [text savi.savi_attach_info.sa_leaf_path];
	td (List.map (fun dmn -> match dmn with Mlvm x -> p [text x.dmn_dm_name]) savi.savi_attach_info.sa_lvs);
	td [text (Int64.to_string savi.savi_phys_size)];
	td [text (match savi.savi_maxsize with Some i -> Int64.to_string i | None -> "None")];
	td [text (Printf.sprintf "%b" savi.savi_activated)];
	]

let current_op_to_table_row id op =
	tr [td [text id]; td [text (Jsonrpc.to_string (Vhd_types.rpc_of_slave_op op))]]

let slave_to_html k metadata =
	let inner = [span [text "SR UUID: "; text metadata.s_data.s_sr; text (if metadata.s_data.s_ready then " (ready)" else " (not ready)") ]] @ 
		(match metadata.s_data.s_master with
			| Some m -> [h 2 [text "Current master"];
			  table ((thead [tr [th [text "uuid"]; th [text "ip"]; th [text "port"]]])::
				  [tr [td [text m.h_uuid]; td [text m.h_ip]; td [text (string_of_int m.h_port)]]])]
			| None -> [h 2 [text "No current master"]]) @ [			  
			h 2 [text "Current attached VDIs"];
			table ((thead [tr [th [text "Location"]; th [text "tapdev"]; th [text "leaf"]; th [text "LVs"]; th [text "Phys size"]; th [text "Max size"]; th [text "Activated"]]])::
				(Hashtbl.fold (fun k v acc -> (attached_vdi_to_table_row k v)::acc) metadata.s_data.s_attached_vdis []));
			h 2 [text "Current ops"];
			table ((thead [tr [th [text "id"]; th [text "op"]]]) :: (Hashtbl.fold (fun k v acc -> (current_op_to_table_row k v)::acc) metadata.s_data.s_current_ops []));
			h 2 [text "Master approved ops"];
			table ((thead [tr [th [text "id"]; th [text "op"]]]) :: (Hashtbl.fold (fun k v acc -> (current_op_to_table_row k v)::acc) metadata.s_data.s_master_approved_ops []))] in
	div (inner)

let status context =
	html [head [title [text "VHDD Status"];
	style ~attrs:["type",Some "text/css"] [text css]];
	body [
		h 1 [text "Debug"];
		div ~attrs:["class",Some "debug"] [p [text (Printf.sprintf "minimum check size: %Ld" !Global.min_size)];
		h 2 [text "Waiting processes"];
		waiting_processes ()];
		h 1 [text "Attached as master"];
		div ~attrs:["class",Some "attached_as_master"] (Attachments.map_master_srs (fun k v -> master_to_html context k v));

		h 1 [text "Attached as slave"];
		div ~attrs:["class",Some "attached_as_slave"] (Attachments.map_slave_srs (fun k v -> slave_to_html k v));
	]] ()

let dot_handler req fd () =
	req.Http.Request.close <- true;
	let context = {
		c_driver="none";
		c_api_call="none";
		c_task_id="none";
		c_other_info=[];
	} in
	let s = fd in
	let path = String.split '/' req.Http.Request.uri in
	match path with
		| ""::"dot"::[sr_uuid] ->
			let metadata = Attachments.gmm sr_uuid in
			let dot = Dot.to_string context metadata in
			let tmp = Filenameext.temp_file_in_dir "/tmp/foo" in
			Unixext.write_string_to_file tmp dot;
			let stdout,stderr = Forkhelpers.execute_command_get_output "/usr/bin/dot" ["-Tpng";tmp] in
			Http_svr.response_str ?hdrs:(Some ["Content-Type","image/png"]) req s stdout
		| _ ->
			failwith "Bad request"

let wait_handler req fd () =
	req.Http.Request.close <- true;
	let path = String.split '/' req.Http.Request.uri in
	match path with
		| ""::"unwait"::[uuid] ->
			(try Nmutex.unwait uuid with _ -> ());
			Http_svr.response_str req fd "OK"
		| _ -> failwith "Bad request"

let status_handler req fd () =
	oddrow := true;
	req.Http.Request.close <- true;
	let context = {
		c_driver="none";
		c_api_call="none";
		c_task_id="none";
		c_other_info=[];
	} in
	let str = status context in
	Http_svr.response_str req fd str

(*********************************************** Metadata change alert *****************************************)

let metadata_change_mutex = Mutex.create ()
let metadata_change_condition = Condition.create ()
let global_id = ref 1

let signal_master_metadata_change metadata () =
	Mutex.execute metadata_change_mutex (fun () ->
		metadata.m_idx <- !global_id;
		incr global_id;
		Condition.broadcast metadata_change_condition)

let signal_slave_metadata_change metadata () =
	Mutex.execute metadata_change_mutex (fun () -> 
		metadata.s_idx <- !global_id;
		incr global_id;
		Condition.broadcast metadata_change_condition)

let update_handler req fd () =
	req.Http.Request.close <- true;
	let id = int_of_string (List.assoc "id" req.Http.Request.query) in
	let get_updates () =
		let all_master = List.filter_map (fun x -> x) 
			(Attachments.map_master_srs (fun k v ->
				if v.m_idx > id then
					Some v
				else
					None))
		in
		let all_slave = List.filter_map (fun x -> x) 
			(Attachments.map_slave_srs (fun k v ->
				if v.s_idx > id then
					Some v
				else
					None))
		in
		let highest_id = List.fold_left (fun cur_max v -> max v.m_idx cur_max) 0 all_master in
		let highest_id = List.fold_left (fun cur_max v -> max v.s_idx cur_max) highest_id all_slave in
		{
			id=highest_id;
			master_metadatas=List.map (fun v -> v.m_data) all_master;
			slave_metadatas=List.map (fun v -> v.s_data) all_slave;
		}	
	in

	let updates = Mutex.execute metadata_change_mutex (fun () ->
		let rec do_it () =
			let updates = get_updates () in
			if (List.length updates.master_metadatas = 0) && (List.length updates.slave_metadatas = 0)
			then
				(Condition.wait metadata_change_condition metadata_change_mutex;
				do_it ())
			else
				updates
		in do_it ())
	in

	let text = Jsonrpc.to_string (rpc_of_updates updates) in

	Http_svr.response_str req ~hdrs:["Content-Type","application/json"] fd text
