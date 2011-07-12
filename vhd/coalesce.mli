(* Coalesce micro ops. Used by normal coalesce and also leaf coalesce *)

val with_coalesce_lock : Smapi_types.context -> Vhd_types.master_sr_metadata ->
	(unit -> unit) -> unit

(** Resize the parent of a VHD to contain the data stored within the
	child VHD as well as itself. Arguments are the context, the master
	metadata, a reservation mode override, a bool option and a
	vhduid. The bool option represents whether the VHD is a leaf, and
	if so, whether it's attached RW or not. None means not a leaf,
	Some true means it's a leaf and attached RW, and Some false means
	it's a leaf but either attached RO or not attached at all. Assumes
	the LVs are not activated when called. *)
val resize_parent : Smapi_types.context -> Vhd_types.master_sr_metadata ->
	Vhdutil.reservation_mode option -> bool option -> string -> unit

(** Move data - calls libvhd to move all of the data from a VHD into
	its parent. Once this is done, it sets the hidden value to '2'.
	The arguments are the context and the path to the VHD. Assumes the
	LVs containing both the VHD and its parent are active.  Returns
	the number of blocks that were moved. *)
val move_data : Smapi_types.context -> string -> int

(** Attach the VHD and move its data to its parent. Taked the 
	context, metadata, vhduid, and returns the vhduid of the parent *)
val attach_and_move_data : Smapi_types.context -> Vhd_types.master_sr_metadata ->
	string -> (int * Vhd_records.pointer)

(** Relink children of a VHD. Takes the context, master metadata, and
	the vhduid. Repoints the parent field of all of the children o
	point to their grandparent. It then reattaches any attached VDIs
	that had the VHD in their chain. It then resizes the grandparent
	and reattaches again. *)
val relink_children : Smapi_types.context -> Vhd_types.master_sr_metadata ->
	string -> unit

(** Helper function that combines the resize parent and move data *)
val resize_and_move_data : Smapi_types.context -> Vhd_types.master_sr_metadata ->
	Vhdutil.reservation_mode option -> bool option -> string -> (int * Vhd_records.pointer)

(** Type representing the classification of the contents of an SR *)
type classification_t = {
	(** the UIDs of VHDs that are not leaves and are coalescable *)
	coalescable_vhds : string list;
	(** The IDs of VDIs that are leaf coalescable *)
	leaf_coalescable_ids : string list;
	(** The UIDs of VHDs that are unreachable from the current leaves,
		together with a boolean representing whether they are hidden
		or not *)
	unreachable_vhds : (string * bool) list;
	(** The locations of LVs that are not reachable and a boolean
		representing whether they are hidden or not *)
	unreachable_lvs : (Lvmabs_types.location_info * bool) list;
	(** Broken vhds - vhds without parents *)
	broken_vhds : string list;
	(** Bad parents - not hidden vhds/lvs with children *)
	bad_parents : Vhd_records.pointer list;
}

(** Classify the contents of an SR. Returns coalescable VHDs, leaf
	coalescable IDs, unreachable VHDs and unreachable LVs *)
val classify_sr_contents : Smapi_types.context -> Vhd_types.master_sr_metadata ->
	classification_t

