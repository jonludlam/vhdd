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
val init_lvm : Smapi_types.context -> string list -> Lvmabs_types.container_info
val init_fs : Smapi_types.context -> string -> Lvmabs_types.container_info
val init_origlvm : Smapi_types.context -> string -> string list -> Lvmabs_types.container_info

(** Helper functions *)
val string_of_location_info : Lvmabs_types.location_info -> string
val is_lvm : Smapi_types.context -> Lvmabs_types.container_info -> bool
val container_sr_uuid : Smapi_types.context -> Lvmabs_types.container_info -> string
val location_uuid : Smapi_types.context -> Lvmabs_types.location_info -> string
val maybe_add_pv_ids : Smapi_types.context -> string -> Lvmabs_types.container_info -> unit

(** Query functions - these do not modify the metadata *)
val path : Smapi_types.context -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> string
val exists : Smapi_types.context -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> bool
val with_active_vhd : Smapi_types.context -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> bool -> (Lvmabs_types.location_type -> string -> 'a) -> 'a
val get_attach_info : Smapi_types.context -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> Int_types.lv_attach_info_t option
val scan : Smapi_types.context -> Lvmabs_types.container_info -> (Lvmabs_types.location_info -> 'a option) -> 'a list
val size : Smapi_types.context -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> (int64 * int64 option)
val find_metadata : Smapi_types.context -> Lvmabs_types.container_info -> string -> (Lvmabs_types.container_info * Lvmabs_types.location_info) option
val get_hidden_lvs : Smapi_types.context -> Lvmabs_types.container_info -> Lvmabs_types.location_info list
val get_sr_sizes : Smapi_types.context -> Lvmabs_types.container_info -> int64 * int64 * int64

(** These 8 functions modify the container metadata *)
val create : Smapi_types.context -> Lvmabs_types.container_info -> string -> int64 -> Lvmabs_types.container_info * Lvmabs_types.location_info
val create_raw : Smapi_types.context -> Lvmabs_types.container_info -> string -> int64 -> Lvmabs_types.container_info * Lvmabs_types.location_info
val remove : Smapi_types.context -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> Lvmabs_types.container_info
val commit : Smapi_types.context -> Lvmabs_types.container_info -> Lvmabs_types.container_info
val create_metadata : Smapi_types.context -> Lvmabs_types.container_info -> string -> bool -> Lvmabs_types.container_info * Lvmabs_types.location_info
val resize : Smapi_types.context -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> int64 -> Lvmabs_types.container_info
val add_tag : Smapi_types.context -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> Lvm.Tag.t -> Lvmabs_types.container_info
val remove_tag : Smapi_types.context -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> Lvm.Tag.t -> Lvmabs_types.container_info
(*val resize : Lvmabs_types.container_info -> Lvmabs_types.location_info -> int64 -> Lvmabs_types.container_info*)

(** Finaliser *)
val shutdown : Smapi_types.context -> Lvmabs_types.container_info -> unit

(** Read and write *)
val read : Smapi_types.context -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> string
val write : Smapi_types.context -> Lvmabs_types.container_info -> Lvmabs_types.location_info -> string -> unit


