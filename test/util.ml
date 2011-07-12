
module Int64extentlist = Extentlist.Extentlist(struct type t=int64 with rpc let add=Int64.add let sub=Int64.sub end)

let write_char file c start len =
  Printf.printf "write_char: %s %c %Ld %d\n%!" file c start len;
  if len > Sys.max_string_length then failwith "len too large";
  let s = String.make len c in
  let fd = Unix.openfile file [Unix.O_RDWR] 0o000 in
  ignore(Unix.LargeFile.lseek fd start Unix.SEEK_SET);
  Unixext.really_write fd s 0 len;
  Unix.close fd

let exec_vhd_util args =
  let stdout, stderr = Forkhelpers.execute_command_get_output "/usr/sbin/vhd-util" args in
  (stdout, stderr)

let write_junk file size n current_junk lv_leaf_path =
  let maxsize = Sys.max_string_length in
  let maxsizeL = Int64.of_int maxsize in
  let rec inner m cur =
	  if m=n then cur else
		  let char = Char.chr (Random.int 255) in
		  let start = Random.int64 (Int64.sub size maxsizeL) in
		  let len = Random.int maxsize in
		  write_char file char start len;
		  if ((m*100)/n) > (((m-1)*100)/n) then
			  Printf.printf "\n%d%%%!" ((m*100)/n);
		  Printf.printf ".%!";
		  (match lv_leaf_path with 
			  | Some leaf_path -> 
				  let (stdout, stderr) = exec_vhd_util ["query"; "-n"; leaf_path; "-p"] in
				  Printf.printf "parent path: %s\n%!" stdout   
			  | None -> ());	  
		  let myextentlist = [(start,Int64.of_int len)] in
		  inner (m+1) ((myextentlist,char)::(List.map (fun (extlist,c) -> (Int64extentlist.difference extlist myextentlist, c)) cur))
  in
  inner 0 current_junk

let check_junk file junk =
	let fd = Unix.openfile file [Unix.O_RDONLY] 0o000 in
	Pervasiveext.finally (fun () -> 
		let rec inner j =
			match j with 
				| (extentlist,c)::rest ->
					Printf.fprintf stderr "Checking char code=%d (no. of extents: %d)\n%!" (Char.code c) (List.length extentlist);
					List.iter (fun (start,len64) -> 
						let len = Int64.to_int len64 in
						ignore(Unix.LargeFile.lseek fd start Unix.SEEK_SET);
						let s = Unixext.really_read_string fd len in
						let check = String.make len c in
						Printf.fprintf stderr "String at pos %Ld len %Ld..." start len64;
						assert(String.compare s check = 0);
						Printf.fprintf stderr "OK!\n%!") extentlist;
					inner rest
				| _ -> ()
		in 
		try
			inner junk 
		with e -> 
			Printf.fprintf stderr "Not OK!\n%!";
			raise e)
		(fun () ->
			Unix.close fd)
	

let pattern n =
  match n with 
    | 1 -> String.make 512 (Char.chr 255)
    | 2 -> 
		let s = String.make 512 (Char.chr 0x77) in
		s
	| _ -> failwith "Unknown pattern"

let write_pattern file ty start len =
  let fd = Unix.openfile file [Unix.O_RDWR] 0o000 in
  let p = pattern ty in
  ignore(Unix.LargeFile.lseek fd start Unix.SEEK_SET);
  for i=1 to len do
    Unixext.really_write fd p 0 512
  done;
  Unix.close fd
    
let read_pattern file ty start len =
  let fd = Unix.openfile file [Unix.O_RDWR] 0o000 in
  let p = pattern ty in
  let p2 = String.create 512 in
  ignore(Unix.LargeFile.lseek fd start Unix.SEEK_SET);
  for i=1 to len do
    Unixext.really_read fd p2 0 512;
    assert (p=p2)
  done;
  Unix.close fd

let copy src dst size =
  let srcfd = Unix.openfile src [Unix.O_RDONLY] 0o000 in
  let dstfd = Unix.openfile dst [Unix.O_WRONLY] 0o000 in
  ignore(Unixext.copy_file ~limit:size srcfd dstfd);
  Unix.close srcfd;
  Unix.close dstfd
