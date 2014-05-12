open Xstringext

module D=Debug.Make(struct let name="iscsilib" end)
open D

let initiator_name = "/etc/iscsi/initiatorname.iscsi"

let get_current_initiator_name () =
	try
		let lines = String.split '\n' (Unixext.string_of_file initiator_name) in
		let line = List.find (fun line -> String.has_substr line "InitiatorName") lines in
		let name = String.strip String.isspace (String.concat "=" (List.tl (String.split '=' line))) in
		Some name
	with _ -> None

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

let iqn_regex = Re.compile (Re_emacs.re "\\([0-9\\.]*\\):\\([0-9]*\\),\\([0-9]*\\)")

let probe_iqns target_address target_port =
  let args = [ "--mode"; "discoverydb"; "--type"; "sendtargets"; "--portal"; Printf.sprintf "%s:%d" target_address target_port; "--discover" ] in
  let result = execute_command_get_output [||] "iscsiadm" args in
  let lines = String.split '\n' result in
  let lines = List.map (fun line -> 
    let parts = String.split ' ' line in
    match parts with
    | [ip_and_group; iqn] -> begin
      try
	let substrings = Re.exec iqn_regex ip_and_group in
	let ip = Re.get substrings 1 in
	let port = Re.get substrings 2 in
	let group = Re.get substrings 3 in
	Some (ip, int_of_string port, int_of_string group)
      with Not_found -> None
    end
    | _ -> None) lines in
  List.fold_left (fun acc x -> match x with Some y -> y::acc | None -> acc) [] lines
