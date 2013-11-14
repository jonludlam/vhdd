module D=Debug.Make(struct let name="nfs" end)
open D

let mount server serverpath localpath transport =
	Unixext.mkdir_rec localpath 0o700;
	let mount_server_arg = Printf.sprintf "%s:%s" server serverpath in
	(*  let options_arg = Printf.sprintf "soft,timeo=%d,retrans=2147483647,%s,noac" (int_of_float ((40.0 /. 3.0) *. 10.0)) transport in*)
	let stdout, stderr = Forkhelpers.execute_command_get_output "/bin/mount" [mount_server_arg; localpath; (*"-o"; options_arg *)] in
	if String.length stderr > 0 then begin
		warn "Warning: Nfs mount returned stderr: %s stdout: %s" stderr stdout
	end;
	localpath

let unmount localpath =
	let stdout, stderr = Forkhelpers.execute_command_get_output "/bin/umount" [localpath;] in
	if String.length stderr > 0 then begin
		warn "Warning: Nfs mount returned stderr: %s stdout: %s" stderr stdout
	end;
	()
