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
  cs : Cstruct.t;
  vhd_link : string;
  mutable next_db : Int32.t
}

let h = Hashtbl.create 50
let m = Mutex.create ()

let register (sr,id) vhd_link =
  let f = Unix.openfile (Printf.sprintf "/dev/shm/%s.stats" (Filename.basename vhd_link)) [Unix.O_RDWR] 0 in
  try
    let ba = Bigarray.Array1.map_file f Bigarray.char Bigarray.c_layout true 4096 in
    let cs = Cstruct.of_bigarray ba in
    Mutex.execute m (fun () ->
      Hashtbl.replace h (sr,id) {cs; next_db=0l; vhd_link;});
    Unix.close f;
    debug "Registered to listen to /dev/shm/%s" vhd_link
  with e ->
    Unix.close f;
    raise e
      
let unregister (sr,id) =
  debug "Unregistering %s/%s" sr id;
  Mutex.execute m (fun () ->
    Hashtbl.remove h (sr,id))

let oneshot () =
  Hashtbl.iter (fun (sr,vdi) st ->
    let len = Int32.to_int (get_tapdisk_stats_len st.cs) in
    let crc = get_tapdisk_stats_checksum st.cs in
    let str = String.make len '\000' in
    Cstruct.blit_to_string (get_tapdisk_stats_msg st.cs) 0 str 0 len;
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

let start () =
  Thread.create loop ()

