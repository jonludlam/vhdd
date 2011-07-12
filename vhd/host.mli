(** Responsible for attaching and detaching LVs for the host.

	The module is shared amongst all SRs that are attached on this host. Implements refcounting
	so that if an LV is attached twice, it must be removed twice before the device-mapper entry
	goes away.

	On daemon restart, it's therefore important that the refcounts are correctly initialised.

	Uses the PV ID to determine which block device the LVs are resident on. Before using 
	attach these should be set up with a call to add_pv_id_info.

	Functions in this module can be safely called in parallel.
*)
	

(** Adds PV ID info. Takes an sr_uuid as a reference, and a map from pv_id -> block device *)
val add_pv_id_info : string -> (string * string) list -> unit

(** Remove PV ID info. Use the sr_uuid that was passed to add_pv_id_info *)
val remove_pv_id_info : string -> unit

(** Changes the device-mapper entry without removing it *)
val change_lv : Int_types.lv_attach_info_t -> unit

(** Used to initialise the refcounts on daemon restart. Each call to bump increases the refcount by 1 *)
val bump_refcount : Int_types.lv_attach_info_t -> unit

(** Attach an LV (or increase its refcount) *)
val attach_lv : Int_types.lv_attach_info_t -> string

(** Remove an LV (or decrease its refcount) *)
val remove_lv : Int_types.lv_attach_info_t -> unit

(** Attaches an LV, calls a function and then guarantees to detach it *)
val with_active_lv : Int_types.lv_attach_info_t -> (string -> 'a) -> 'a
