module SC=Smapi_client
open Smapi_types

let meg = Int64.mul 1024L 1024L

let simpletest all sr = 
  let (host, master, gp, rpc, intrpc) = List.hd all in
  let vdi = (SC.VDI.create rpc gp sr [] (Int64.mul meg 50L)).vdi_info_location in
  let leaf_path = SC.VDI.attach rpc gp sr vdi in
  SC.VDI.activate rpc gp sr vdi;
  
  (* Create a SMAPI call that fails *)
  ignore(try
	  SC.VDI.activate rpc gp sr "foobar"
  with _ -> ());

  Pervasiveext.finally 
	  (fun () -> 
		  let junk = Util.write_junk leaf_path (Int64.mul meg 50L) 20 [] None in
		  let junk2 = Util.write_junk leaf_path (Int64.mul meg 50L) 2 junk None in
		  Util.check_junk leaf_path junk2)
	  (fun () -> 
		  Printf.fprintf stderr "About to deactive\n%!";
		  SC.VDI.deactivate rpc gp sr vdi;
		  Printf.fprintf stderr "About to detach\n%!";
		  SC.VDI.detach rpc gp sr vdi)
