open Smapi_types
open Util

module SC=Smapi_client

    
let check (file1,file2,size,rd,wr2) =
  let buf1 = String.make 65536 '\000' in
  let buf2 = String.make 65536 '\000' in
  while true do
    let fd1 = Unix.openfile file1 [Unix.O_RDONLY; Unix.O_DSYNC; Unix.O_RSYNC] 0o000 in
    let fd2 = Unix.openfile file2 [Unix.O_RDONLY; Unix.O_DSYNC; Unix.O_RSYNC] 0o000 in
    try
		ignore(Unix.LargeFile.lseek fd1 0L Unix.SEEK_SET);
		ignore(Unix.LargeFile.lseek fd2 0L Unix.SEEK_SET);
      
      for i=0 to (Int64.to_int (Int64.div size 65536L))-1 do
	let (ready,_,_) = Unix.select [rd] [] [] 0.0 in
	if ready=[] then begin
	  Unixext.really_read fd1 buf1 0 65536;
	  Unixext.really_read fd2 buf2 0 65536;
	  assert (buf1=buf2);
	  Printf.fprintf stderr ".%!"
	end else begin
	  if Unixext.really_read_string rd 2 = "OK" then begin
	    Unix.close fd1;
	    Unix.close fd2;
	    ignore(Unix.write wr2 "OK" 0 2);
	    Thread.exit ()
	  end else begin
	    Printf.fprintf stderr "Ack! error!\n";
	    Thread.exit ()
	  end
	end
      done;
      Printf.fprintf stderr "Finished one check\n%!";
      Unix.close fd1;
      Unix.close fd2
    with e ->
      Printf.fprintf stderr "Exception raised: %s\n%!" (Printexc.to_string e);
      Unix.close fd1;
      Unix.close fd2
  done

let meg = Int64.mul 1024L 1024L

let coalesce_test all sr = 
  let (host, master, gp, rpc, intrpc) = List.hd all in
  let vdi = (SC.VDI.create rpc gp sr [] (Int64.mul meg 50L)).vdi_info_location in
  let leaf_path = SC.VDI.attach rpc gp sr vdi in
  SC.VDI.activate rpc gp sr vdi;
  
  let clones = ref [] in

  let sr_uuid = match sr with Some f -> f.sr_uuid | None -> failwith "Need a valid SR!" in

  let lv_leaf_path = Int_client.Debug.vdi_get_leaf_path intrpc sr_uuid vdi in
  
  ignore(write_junk leaf_path (Int64.mul meg 50L) 10 [] (Some lv_leaf_path));

(*  let foo = SC.VDI.clone rpc gp sr [] vdi in
  let bar = SC.VDI.clone rpc gp sr [] foo.vdi_info_location in
  let baz = SC.VDI.clone rpc gp sr [] bar.vdi_info_location in*)
  
(*  Printf.printf "Sleeping for a bit (check parents etc!)\n";
  Thread.delay 30.0;*)

  for i=0 to 2 do
    write_pattern leaf_path 1 0L 100;
    let clone = (SC.VDI.clone rpc gp sr [] vdi).vdi_info_location in
    read_pattern leaf_path 1 0L 100;
    Printf.printf "pattern OK!\n%!";
    let lv_leaf_path = Int_client.Debug.vdi_get_leaf_path intrpc sr_uuid vdi in
    ignore(write_junk leaf_path (Int64.mul meg 50L) 10 [] (Some lv_leaf_path));
    clones := clone :: !clones;  
  done;
  
  let vdi2 = (SC.VDI.create rpc gp sr [] (Int64.mul meg 50L)).vdi_info_location in
  let leaf_path2 = SC.VDI.attach rpc gp sr vdi2 in
  SC.VDI.activate rpc gp sr vdi2;

  (* A couple more clones for good measure... *)



  
  Printf.printf "Copying disk:\n%!";
  copy leaf_path leaf_path2 (Int64.mul meg 50L);
  
  Printf.printf "Finished. Starting check thread:\n%!";
  let (rd,wr) = Unix.pipe () in
  let (rd2,wr2) = Unix.pipe () in
  
  ignore(Thread.create check (leaf_path,leaf_path2,Int64.mul meg 50L,rd,wr2));
  
  (*for i=0 to 2 do*)
    let clone = List.hd !clones in
    clones := List.tl (!clones);
    SC.VDI.delete rpc gp sr clone;
    ignore(SC.SR.scan rpc gp sr);
  (*done;*)

(*  SC.VDI.delete rpc gp sr foo.vdi_info_location;
  SC.VDI.delete rpc gp sr bar.vdi_info_location; *)
	ignore(SC.SR.scan rpc gp sr);
  
  let response = String.create 2 in
  
  (*  Printf.printf "Inserting error to check check thread\n%!";
      
  write_junk leaf_path (Int64.mul meg 50L) 1 lv_leaf_path;

  Thread.delay 10.0;*)

  Printf.printf "Closing check thread\n%!";
  ignore(Unix.write wr "OK" 0 2);
  Printf.printf "Waiting for response\n%!";
  ignore(Unix.read rd2 response 0 2);
  Printf.printf "Done: response=%s\n%!" response;

  Printf.printf "Pausing for a little bit...\n%!";
  Thread.delay 10.0;
  Printf.printf "Deactivating/detaching...\n%!";

  SC.VDI.deactivate rpc gp sr vdi;
  SC.VDI.detach rpc gp sr vdi;

  SC.VDI.deactivate rpc gp sr vdi2;
  SC.VDI.detach rpc gp sr vdi2;
  

