(** Interface to the standard LVM utilities.
	
	This can be used in two 'modes' - the first is 'standard' where the LVM tools are used
	for all device-mapper interactions, and the second mode is where Camldm is used to 
	activate and deactivate the device-mapper nodes.

	The three functions that are of interest for the two different modes are:
	activate_lv, deactivate_lv and dm_map_of_lv. The first two use the standard LVM tools
	to create and destroy the device-mapper node, and the third extracts the information required
	to activate/deactivate via Camldm.
*)

(** The opaque type that represents a volume group *)
type vg

(** The opaque type that represents a logical volume *)
type lv

(** Initialise the vg type based on the sr_uuid passed, and searching only on the devices passed in.
	@param config_name The name a directory under which the LVM config file is created
    @param devices The list of devices on which the SR is expected to be found ('/dev/sda' etc) 
	@return Returns a vg type *)
val init : string -> string list -> vg

(** Given a device, return the name of the VG inhabiting the device, or None if there isn't one *)
val get_vg_name_from_device : string -> string option

(** Create a volume group on a list of devices
	@param config_name The name a directory under which the LVM config file is created
	@param vg_name The name to give the VG
	@param devices The list of devices on which the SR is expected to be found ('/dev/sda' etc)
	@return Returns a vg type *)
val create_vg : string -> string -> string list -> unit


(** Get the device-mapper name representing the LV *)
val get_dm_name : vg -> lv -> string

(** Get the path of the device-mapper node that, when it exists, will point to the LV block device *)
val get_dm_path : vg -> lv -> string

(** Get a mapping from device -> pv_id *)
val get_pv_ids : vg -> (string * string) list

(** Get the VG name *)
val get_vg_name : vg -> string

(** Get the LV name *)
val get_lv_name : lv -> string

(** Get a list of all the LVs present in the VG *)
val get_lvs : vg -> lv list

(** Get an LV from a VG with the given name. Returns None if there isn't one *)
val get_lv_by_name : vg -> string -> lv option

(** Get a list of all LVs with a particular tag *)
val get_lvs_with_tag : vg -> Lvm.Tag.t -> lv list

(** Get the sizes of a VG
	@return returns a 3-tuple of total bytes, used bytes and free bytes *)
val get_vg_sizes : vg -> int64 * int64 * int64

(** Get the size of a particular LV *)
val get_lv_size : vg -> lv -> int64

(** Return the device-mapper table for a particular LV. This can be passed to Camldm to activate the logical
	volume. See also activate_lv and deactivate_lv *)
val dm_map_of_lv : vg -> lv -> Camldm.mapping array

(** Activate a logical volume using the LVM tools *)
val activate_lv : vg -> lv -> unit

(** Deactivate a logical volume using the LVM tools *)
val deactivate_lv : vg -> lv -> unit

(** Refresh a logical volume if the metadata representing the LV has changed *)
val change_lv : vg -> lv -> unit

(** Add a tag to an LV *)
val add_tag : vg -> lv -> Lvm.Tag.t -> vg

(** Remove a tag from an LV *)
val remove_tag : vg -> lv -> Lvm.Tag.t -> vg

(** Create an LV. Note that this is a functional implementation, and the volume group passed in will not reflect
	the change. Therefore use the vg returned.
	@return Returns a tuple of the modified vg and the new lv *)
val create_lv : vg -> string -> int64 -> vg * lv

(** Check whether an LV exists or not within the specified VG *)
val lv_exists : vg -> lv -> bool

(** Destroy the specified LV. Note that this is a functional implementation, and the volume group passed in will not reflect
	the change. Therefore use the vg returned.
	@return Returns the modified vg *)
val destroy_lv : vg -> lv -> vg

(** Rename the specified LV. Note that this is a functional implementation, and the volume group passed in will not reflect
	the change. Therefore use the vg returned.
	@return Returns a tuple of the modified vg and the lv *)
val rename_lv : vg -> lv -> string -> vg * lv
val resize_lv : vg -> lv -> int64 -> vg


(**/**)
val vg_of_rpc : Rpc.t -> vg
val rpc_of_vg : vg -> Rpc.t
val lv_of_rpc : Rpc.t -> lv
val rpc_of_lv : lv -> Rpc.t

