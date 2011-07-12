open Stringext

let initiator_name = "/etc/iscsi/initiatorname.iscsi"

let get_current_initiator_name () =
	try
		let lines = String.split '\n' (Unixext.string_of_file initiator_name) in
		let line = List.find (fun line -> String.has_substr line "InitiatorName") lines in
		let name = String.strip String.isspace (String.concat "=" (List.tl (String.split '=' line))) in
		Some name
	with _ -> None

