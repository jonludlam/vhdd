(** LVM Abstraction layer 

	This module is an abstraction layer between file-based VHDs and LVM-based VHDs

	A 'container' is a container of VHDs and metadata 
	A 'location' is a reference to a 'thing' within the container, which might be
	a VHD, a RAW LV or metadata.
*)


exception Rtte of (Lvmabs_types.container_info * Lvmabs_types.location_info)
exception Vhd_not_found of (Lvmabs_types.container_info * Lvmabs_types.location_info)
exception Container_is_not_an_sr of Lvmabs_types.container_info


(** Creation functions *)
val init_lvm : Context.t -> string list -> Lvmabs_types.container_info
val init_fs : Context.t -> string -> Lvmabs_types.container_info
val init_origlvm : Context.t -> string -> string list -> Lvmabs_types.container_info

(** Helper functions *)
val string_of_location_info : Lvmabs_types.location_info -> string
val is_lvm : Context.t -> Lvmabs_types.container_info -> bool
val container_sr_uuid : Context.t -> Lvmabs_types.container_info -> string
val location_uuid : Context.t -> Lvmabs_types.location_info -> string
val maybe_add_pv_ids : Context.t -> string -> Lvmabs_types.container_info -> unit

(** Query functions - these do not modify the metadata *)
val path : Context.t -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> string
val exists : Context.t -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> bool
val with_active_vhd : Context.t -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> bool -> (Lvmabs_types.location_type -> string -> 'a) -> 'a
val get_attach_info : Context.t -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> Int_types.lv_attach_info_t option
val scan : Context.t -> Lvmabs_types.container_info -> (Lvmabs_types.location_info -> 'a option) -> 'a list
val size : Context.t -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> (int64 * int64 option)
val find_metadata : Context.t -> Lvmabs_types.container_info -> string -> (Lvmabs_types.container_info * Lvmabs_types.location_info) option
val get_hidden_lvs : Context.t -> Lvmabs_types.container_info -> Lvmabs_types.location_info list
val get_sr_sizes : Context.t -> Lvmabs_types.container_info -> int64 * int64 * int64

(** These 8 functions modify the container metadata *)
val create : Context.t -> Lvmabs_types.container_info -> string -> int64 -> Lvmabs_types.container_info * Lvmabs_types.location_info
val create_raw : Context.t -> Lvmabs_types.container_info -> string -> int64 -> Lvmabs_types.container_info * Lvmabs_types.location_info
val remove : Context.t -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> Lvmabs_types.container_info
val commit : Context.t -> Lvmabs_types.container_info -> Lvmabs_types.container_info
val create_metadata : Context.t -> Lvmabs_types.container_info -> string -> bool -> Lvmabs_types.container_info * Lvmabs_types.location_info
val resize : Context.t -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> int64 -> Lvmabs_types.container_info
val add_tag : Context.t -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> Lvm.Tag.t -> Lvmabs_types.container_info
val remove_tag : Context.t -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> Lvm.Tag.t -> Lvmabs_types.container_info
(*val resize : Lvmabs_types.container_info -> Lvmabs_types.location_info -> int64 -> Lvmabs_types.container_info*)

(** Finaliser *)
val shutdown : Context.t -> Lvmabs_types.container_info -> unit

(** Read and write *)
val read : Context.t -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> string
val write : Context.t -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> string -> unit


