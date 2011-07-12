open Threadext
open Smapi_types

module SC=Smapi_client
module D=Debug.Debugger(struct let name="parallel_test" end)
open D

let leaves = ref [] 
let lock = Mutex.create () 

let meg = Int64.mul 1024L 1024L

exception No_vdis

let get_random_vdi () =
  Mutex.execute lock (fun () ->
    let n = List.length !leaves in
	if n=0 then raise No_vdis;
    let m = Random.int n in
    let vdi = List.nth !leaves m in
    vdi) 

let remove_random_vdi () =
  Mutex.execute lock (fun () ->
    let n = List.length !leaves in
    let m = Random.int n in
    let vdi = List.nth !leaves m in
    leaves := List.filter (fun vdi' -> vdi <> vdi') !leaves;
    vdi) 

let do_op rpc gp sr is_master =
  match Random.int (if is_master then 6 else 2) with
    | 0 -> (* Attach/activate disk *)
	let vdi = remove_random_vdi () in
	Pervasiveext.finally 
		(fun () -> 
			debug "VDI.activate/attach (%s)" vdi;
			let path = SC.VDI.attach rpc gp sr vdi in
			ignore(SC.VDI.activate rpc gp sr vdi);
			ignore(Util.write_junk path (Int64.mul meg 1L) 3 [] None);
			Thread.delay 1.0) 
		(fun () -> 
			Mutex.execute lock (fun () -> 
				leaves := vdi :: !leaves)) (* add it back in *)
    | 1 -> (* Detach/deactivate disk *)
	let vdi = get_random_vdi () in
	debug "VDI.deactivate/detach (%s)" vdi;
	(try ignore(SC.VDI.deactivate rpc gp sr vdi) with _ -> ());
	(try ignore(SC.VDI.detach rpc gp sr vdi) with _ -> ())
    | 2 -> (* Create disk *)
	debug "VDI.create";
	let vdi = (SC.VDI.create rpc gp sr [] (Int64.mul meg 50L)).vdi_info_location in
	Mutex.execute lock (fun () -> 
	  leaves := vdi :: !leaves)
    | 3 -> (* Delete disk *)
	let vdi = get_random_vdi () in
	debug "VDI.delete (%s)" vdi;
	ignore(SC.VDI.delete rpc gp sr vdi);
	Mutex.execute lock (fun () ->
	  leaves := List.filter (fun vdi' -> vdi' <> vdi) !leaves)
    | 4 -> (* Clone disk *)
	let vdi = get_random_vdi () in
	debug "VDI.clone (%s)" vdi;
	let vdi2 = (SC.VDI.clone rpc gp sr [] vdi).vdi_info_location in
	Mutex.execute lock (fun () ->
	  leaves := vdi2 :: !leaves);
    | 5 -> (* Scan *)
	debug "SR.scan";
	ignore(SC.SR.scan rpc gp sr)
	| _ -> failwith "Undefined operation!"
	
let operation_thread rpc gp sr is_master rd wr2 () =
  debug "Thread started";
  while true do
    try
      let (ready,_,_) = Unix.select [rd] [] [] 0.0 in
      if ready=[] then begin
	do_op rpc gp sr is_master
      end else begin
	let str = Unixext.really_read_string rd 2 in
	if str = "OK" then begin
      debug "Received exit message";
	  ignore(Unix.write wr2 "OK" 0 2);
	  Thread.exit ()
	end else begin
	  debug "Received odd message: %s" str;
	end
      end
    with
		|No_vdis ->
			debug "Waiting for some VDIs to turn up";
			Thread.delay 1.0
		| e ->
			debug "Caught exception: %s" (SC.e_to_string e);
			debug "Pausing for 1 seconds";
			Thread.delay 1.0
  done

let paralleltest all sr =
  
  let n = 10 in

  let rec do_host channels (host, is_master, gp, rpc, intrpc) =
    let rec start_thread channels m = 
      if m=0 then channels else begin
	let (rd,wr) = Unix.pipe () in
	let (rd2,wr2) = Unix.pipe () in
	
	ignore(Thread.create (operation_thread rpc gp sr is_master rd wr2) ());
	
	start_thread ((rd2,wr)::channels) (m-1)
      end
    in start_thread channels n 
  in
  
  let channels = List.fold_left do_host [] all in

  debug "Delaying for 60 seconds";

  Thread.delay 60.0;

  let response = String.create 2 in
  
  List.iter (fun (rd2,wr) ->
    ignore(Unix.write wr "OK" 0 2);
  ) channels;

  List.iter (fun (rd2,wr) ->
    ignore(Unix.read rd2 response 0 2);
    assert(response="OK")
  ) channels;

  debug "Finished. Cleaning up.";

  List.iter (fun (h, is_master, gp, rpc, intrpc) -> 
    List.iter (fun vdi ->
      debug "Deactivating on %s vdi %s" h vdi;
      (try SC.VDI.deactivate rpc gp sr vdi with _ -> ());
      debug "Detaching on %s vdi %s" h vdi;
      (try SC.VDI.detach rpc gp sr vdi with _ -> ());
      debug "Done"
    ) !leaves) all;

  let (_,_,gp,rpc,_) = List.hd all in

  List.iter (fun vdi ->
	  debug "Deleting VDI %s" vdi;
      (try SC.VDI.delete rpc gp sr vdi with _ -> ())) !leaves;
  
  (try ignore(SC.SR.scan rpc gp sr) with _ -> ());

  debug "Done"
