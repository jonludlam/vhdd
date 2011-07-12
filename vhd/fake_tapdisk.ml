(* Fakes the tapdisk *)

let _ =
	let vhd = ref "" in
	let base_dir = ref "" in
	let args =
		[ "-n",Arg.Set_string vhd,"Set the VHD file";
		"-b",Arg.Set_string base_dir,"Set the base directory";]
	in
	Arg.parse args (fun s -> ()) "Usage: Read the source!";
	if not Stringext.startswith "vhd:" !vhd then
		failwith "Can only handle vhd: types"
