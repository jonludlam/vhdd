(* Thread to listen to shared memory that tapdisk uses as comms *)

open Stringext
open Threadext

module D=Debug.Make(struct let name="tapdisk_listen" end)
open D

cstruct tapdisk_stats {
  uint32_t len;
  uint32_t checksum;
  uint8_t msg[256]
} as little_endian

type td_info = {
  read_cs : Cstruct.t;
  write_cs : Cstruct.t;
  vhd_link : string;
  mutable next_db : Int32.t
}

let h = Hashtbl.create 50
let m = Mutex.create ()

let get_stats_fname vhd_link =
  Printf.sprintf "/dev/shm/%s.stats" (Filename.basename vhd_link)

let exists name = try ignore (Unix.stat name); true with _ -> false

let register (sr,id) vhd_link =
  let fname = get_stats_fname vhd_link in
  let f =
    if !Global.dummy && not (exists fname)
    then begin
      let f = Unix.openfile fname [Unix.O_RDWR; Unix.O_CREAT] 0o666 in
      let written = Unix.write f (String.make 4096 '\000') 0 4096 in
      (if written != 4096 then failwith "Error creating shared mem");
      ignore(Unix.lseek f 0 Unix.SEEK_SET);
      f
    end else begin
      Unix.openfile fname [Unix.O_RDWR] 0
    end
  in
  try
    let ba = Bigarray.Array1.map_file f Bigarray.char Bigarray.c_layout true 4096 in
    let cs = Cstruct.of_bigarray ba in
    let read_cs = Cstruct.sub cs 0 2048 in
    let write_cs = Cstruct.sub cs 2048 2048 in
    Mutex.execute m (fun () ->
      Hashtbl.replace h (sr,id) {read_cs; write_cs; next_db=0l; vhd_link;});
    Unix.close f;
    debug "Registered to listen to %s" fname
  with e ->
    Unix.close f;
    raise e
      
let unregister (sr,id) =
  debug "Unregistering %s/%s" sr id;
  Mutex.execute m (fun () ->
    begin
      try
	if !Global.dummy
	then begin 
	  let x = Hashtbl.find h (sr,id) in
	  Unixext.unlink_safe (get_stats_fname x.vhd_link);
	end
      with _ -> ()
    end;
    Hashtbl.remove h (sr,id))

let oneshot () =
  Hashtbl.iter (fun (sr,vdi) st ->
    let len = Int32.to_int (get_tapdisk_stats_len st.read_cs) in
    let crc = get_tapdisk_stats_checksum st.read_cs in
    let str = String.make len '\000' in
    Cstruct.blit_to_string (get_tapdisk_stats_msg st.read_cs) 0 str 0 len;
    let crc' = Zlib.update_crc Int32.zero str 0 len in
    if crc=crc' then begin
      let next_db = Int32.of_string str in
      if next_db <> st.next_db then begin
	st.next_db <- next_db;
	let size = Int64.of_int32 next_db in
	debug "Got an update: next_db = %Ld" size;
	let start_of_chunk_in_bytes = Int64.mul 512L size in
	let end_of_chunk_in_bytes = Int64.add start_of_chunk_in_bytes 2097152L in
	let end_of_chunk_including_bitmap = Int64.add end_of_chunk_in_bytes 4096L in
	let with_footer = Int64.add end_of_chunk_including_bitmap 512L in
	Int_client.LocalClient.VDI.slave_set_phys_size ~sr ~vdi ~size:with_footer;
      end
    end) h

let rec loop () =
  try
    Thread.delay 1.0;
    oneshot ();
    loop ()
  with e ->
    debug "Caught exception in Tapdisk_listen.loop: %s" (Printexc.to_string e);
    loop ()

let write_maxsize (sr,id) maxsize =
  let without_footer = Int64.sub maxsize 512L in
  let without_bitmap = Int64.sub without_footer 4096L in
  let start_of_chunk = Int64.sub without_bitmap 2097152L in
  let sector_pos = Int64.div start_of_chunk 512L in
  let next_db = Int64.to_int32 sector_pos in
  let st = Mutex.execute m (fun () -> Hashtbl.find h (sr,id)) in
  let str = Printf.sprintf "%ld" next_db in
  let len = String.length str in
  let crc = Zlib.update_crc Int32.zero str 0 len in
  Cstruct.blit_from_string str 0 (get_tapdisk_stats_msg st.write_cs) 0 len;
  set_tapdisk_stats_checksum st.write_cs crc;
  set_tapdisk_stats_len st.write_cs (Int32.of_int len)

let debug_write (sr,id) next_db =
  let st = Mutex.execute m (fun () -> Hashtbl.find h (sr,id)) in
  let str = Printf.sprintf "%Ld" next_db in
  let len = String.length str in
  let crc = Zlib.update_crc Int32.zero str 0 len in
  Cstruct.blit_from_string str 0 (get_tapdisk_stats_msg st.read_cs) 0 len;
  set_tapdisk_stats_checksum st.read_cs crc;
  set_tapdisk_stats_len st.read_cs (Int32.of_int len)

let rec debug_read (sr,id) =
  let st = Mutex.execute m (fun () -> Hashtbl.find h (sr,id)) in
  let len = Int32.to_int (get_tapdisk_stats_len st.read_cs) in
  let crc = get_tapdisk_stats_checksum st.read_cs in
  let str = String.make len '\000' in
  Cstruct.blit_to_string (get_tapdisk_stats_msg st.read_cs) 0 str 0 len;
  let crc' = Zlib.update_crc Int32.zero str 0 len in
  if crc=crc' then begin
    let next_db = Int32.of_string str in
    let size = Int64.of_int32 next_db in
    let start_of_chunk_in_bytes = Int64.mul 512L size in
    let end_of_chunk_in_bytes = Int64.add start_of_chunk_in_bytes 2097152L in
    let end_of_chunk_including_bitmap = Int64.add end_of_chunk_in_bytes 4096L in
    let with_footer = Int64.add end_of_chunk_including_bitmap 512L in
    with_footer
  end else debug_read (sr,id)

let start () =
  Thread.create loop ()

