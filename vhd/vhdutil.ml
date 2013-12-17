(* Very similar to vhdutil.py! *)

open Stringext
open Int64

module D = Debug.Make(struct let name="vhdutil" end)
open D

(* Some utility definitions *)
let kib = 1024L
let mib = mul kib kib
let gib = mul kib mib
let mib_minus_1 = sub mib 1L
let max_size = mul gib 2040L
let max_bat = mul (mul 2040L 1024L) 2L (* Max megs * 2 *)
let two_mib = mul 2L mib
let two_mib_minus_1 = sub two_mib 1L

(* Thin provisioning thresholds *)
let tp_overhead = mul mib 100L
let tp_threshold = mul mib 50L
let tp_lower_threshold = mul mib 75L
let tp_emergency_threshold = mul mib 10L

(* critical size is only used for thin provisioning *)
type size = {
	overhead : int64; (* This is the extra metadata size, not including the 2Mb+bitmap chunks *)
	phys_size : int64; (* The current physical size of the vhd. When the VHD is empty, this is equal to overhead *)
	virtual_size : int64; (* The current virtual size of the vhd *)
	critical_size : int64;
}

and reservation_mode =
	| Leaf
	| Thin
	| Attach
	with rpc

let zero_uuid = Uuidm.to_string Uuidm.nil

(* Utility *)
let roundup v block =
	mul block (div (sub (add v block) 1L) block)

(* Note: the following call is only used for estimating the overhead for initial creation of
   a VHD. Once the VHD is created, we always use libvhd to determine the physical size of the
   VHDs. *)

let calc_lvhd_overhead is_clone use_max_bat virtual_size allocated_size =
	let virtual_megs = mul 2L (div (add virtual_size two_mib_minus_1) two_mib) in (* Round up to next 2M chunk *)
	let allocated_megs = mul 2L (div (add allocated_size two_mib_minus_1) two_mib) in (* Round up to next 2M chunk *)

	(* header = 1024, footer=512, backup footer=512, parent locators = 512 per locator, 3 locators (?) *)
	let header_footer_locators = add (mul 2L 1024L) (if is_clone then mul 3L 512L else 0L) in

	(* BAT is 4 bytes per 2 meg chunk, BAT=size/2 * 4 = size*2 *)
	let bat = if use_max_bat then max_bat else roundup (mul virtual_megs 2L) 512L in

	(* BATMAP is a 28 byte header plus 1 bit per 2 meg chunk, rounded to the next 512L *)
	let batmap = roundup (add (div bat 32L) 28L) 512L in

	(* bitmap is 1 bit per sector in a 2 meg chunk = 512 bytes. Next chunk is page aligned, therefore overhead
	   equals 4096 bytes per 2 meg chunk, or 2048 bytes per 1 meg *)
	let bitmap = mul 2048L allocated_megs in

	(*  Printf.printf "header_footer_locators = %Ld\n" header_footer_locators;
		Printf.printf "bat = %Ld\n" bat;
		Printf.printf "batmap = %Ld\n" batmap;
		Printf.printf "bitmap = %Ld\n" bitmap;*)

	roundup (add (add (add batmap bitmap) bat) header_footer_locators) 512L

let with_vhd nod rw f =
	let vhd = Vhd._open nod (if rw then [Vhd.Open_rdwr] else [Vhd.Open_rdonly]) in
	Pervasiveext.finally (fun () -> f vhd) (fun () -> Vhd.close vhd)

let string_of_size size =
	Printf.sprintf "overhead=%Ld phys_size=%Ld virtual_size=%Ld critical_size=%Ld"
		size.overhead size.phys_size size.virtual_size size.critical_size

let dump_size size =
	debug "Dump size: %s" (string_of_size size)

let query_size_vhd vhd =
	debug "query_size_vhd: Querying size of VHD %s" (Vhd.get_uid vhd);
	let phys_size = Vhd.get_phys_size vhd in
	debug "query_size_vhd: get_phys_size returned %Ld" phys_size;
	let first_allocated_block = Vhd.get_first_allocated_block vhd in
	debug "query_size_vhd: first_allocated_block=%s" (match first_allocated_block with | Some s -> Printf.sprintf "Some %Ld" s | None -> "None");
	let overhead =
		match first_allocated_block with 
			| None -> phys_size
			| Some x -> Int64.add (Int64.mul x 512L) 512L in
	let virtual_size = Vhd.get_virtual_size vhd in
	debug "query_size_vhd: virtual_size=%Ld" virtual_size;
	{ overhead = overhead;
	phys_size = phys_size;
	virtual_size = virtual_size;
	critical_size = Int64.add tp_threshold phys_size}

let query_size nod =
  with_vhd nod false query_size_vhd

(** set_hidden: This takes a Vhd.t and sets the hidden field appropriately. 1==OK for moving data, 2==OK for relinking.
	It returns a tuple of the phys_size and the hidden value *)
let set_hidden vhd =
	let first_allocated_block = Vhd.get_first_allocated_block vhd in
	let hidden_value = match first_allocated_block with | None -> 2 | Some _ -> 1 in
	Vhd.set_hidden vhd hidden_value;
	hidden_value

let get_virtual_size size = size.virtual_size
let get_phys_size size = size.phys_size
let get_critical_size size = size.critical_size

let update_phys_size size new_phys_size =
	{size with phys_size = new_phys_size}

let get_size_for_new_vhd virtual_size =
	let rounded_virtual_size = roundup virtual_size two_mib in
	let overhead = calc_lvhd_overhead true true rounded_virtual_size 0L in
	{ overhead = overhead;
	phys_size = overhead;
	virtual_size = rounded_virtual_size;
	critical_size = Int64.add overhead tp_overhead}

(* Given an amount of real allocated data, this will calculate how much space in addition to the
   VHD overhead is required to store that. We only ever call this function with the parameter
   'virtual_size' in order to determine the largest size the VHD could ever be (for leaf/attach
   provisioning modes) *)
let get_data_size allocated_data =
	let n_two_mib_chunks = Int64.div (Int64.add allocated_data two_mib_minus_1) two_mib in
	(* Each two mib chunk will incur an additional 4k of metadata (512 bytes + padding) of bitmap *)
	let additional_metadata_size = Int64.mul n_two_mib_chunks 4096L in
	let bytes_required_for_chunks = Int64.mul n_two_mib_chunks two_mib in
	Int64.add bytes_required_for_chunks additional_metadata_size

let max_phys_size size =
	let data_size = get_data_size size.virtual_size in
	Int64.add data_size size.overhead

let size_with_thin_provisioning_overhead size =
	let max_phys_size = max_phys_size size in
	let min_phys_size = Int64.add size.phys_size tp_overhead in
	min max_phys_size min_phys_size


(* This will tell us what size to make the LV. *)
let get_lv_size reservation_type leaf_status size =
	dump_size size;
	debug "leaf_status: %s reservation_type: %s" (match leaf_status with None -> "None" | Some b -> Printf.sprintf "Some %b" b)
		(match reservation_type with | None -> "None" | Some Thin -> "Thin" | Some Leaf -> "Leaf" | Some Attach -> "Attach");		
	let reserved_size =
		match reservation_type,leaf_status with
			| None, _ -> 0L
				(* If we're not a LVM type SR, return 0 *)

			| Some _, None -> size.phys_size
				(* any non-leaf VHDs are always deflated down to their phys_size, whatever the allocation policy *)

			| Some Thin, Some _ -> size_with_thin_provisioning_overhead size
				(* Thin provisioned VHDs are sized to be phys_size + tp_overhead, up to the max size. *)

			| Some Leaf, Some _ -> max_phys_size size
				(* leaf VHDs in Leaf-provisioned mode always inflated *)

			| Some Attach, Some true -> max_phys_size size
			| Some Attach, Some false -> size.phys_size
				(* Attach provisioned mode is inflated if it's attached and deflated if it's detached *)
	in
	roundup reserved_size Lvm.Constants.extent_size


(* This is a naive implementation. Really what we should be doing is unioning the BATs and determining
   size from that. What this currently does is an overestimate, which we correct afterwards. *)
let sum_sizes size1 size2 =
	let overhead = min size1.overhead size2.overhead in
	let phys_size = Int64.sub (Int64.add size1.phys_size size2.phys_size) overhead in
	{ overhead = overhead;
	phys_size = phys_size;
	virtual_size = max size1.virtual_size size2.virtual_size;
	critical_size = Int64.add phys_size tp_overhead }

(* Zero out the footer, to prevent libvhd from reading junk and interpreting it as a real footer *)
let wipe_footer nod =
	let fd = Unix.openfile nod [Unix.O_WRONLY; Unix.O_SYNC] 0 in
	Pervasiveext.finally
		(fun () ->
			ignore(Unix.LargeFile.lseek fd (-512L) Unix.SEEK_END);
			Unixext.really_write_string fd (String.make 512 '\000'))
		(fun () -> Unix.close fd)

(* Footer pos is defined as the byte immediately after the end of the 512 byte footer *)
let get_footer_pos_from_phys_size phys_size =
	roundup phys_size Lvm.Constants.extent_size
