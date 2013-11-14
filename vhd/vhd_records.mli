type pointer = PVhd of string | PRaw of Lvmabs_types.location_info

and vhd_record = {
  vhduid : string;
  parent : pointer option;
  location : Lvmabs_types.location_info;
  size : Vhdutil.size;
  hidden : int;
}
and vhd_record_container 

val vhd_record_container_of_rpc : Rpc.t -> vhd_record_container
val vhd_record_of_rpc : Rpc.t -> vhd_record
val pointer_of_rpc : Rpc.t -> pointer
val rpc_of_vhd_record_container : vhd_record_container -> Rpc.t
val rpc_of_vhd_record : vhd_record -> Rpc.t
val rpc_of_pointer : pointer -> Rpc.t
val string_of_pointer : pointer -> string



val of_vhds : vhd_record list -> (pointer * bool) list -> vhd_record_container
val get_vhd : Context.t -> vhd_record_container -> string -> vhd_record
val add_vhd : Context.t -> vhd_record_container -> string -> vhd_record -> unit
val update_vhd_size : Context.t -> vhd_record_container -> string -> Vhdutil.size -> vhd_record
val update_hidden : Context.t -> vhd_record_container -> pointer -> int -> unit
val update_vhd_parent : Context.t -> vhd_record_container -> string -> pointer option -> vhd_record
val remove_vhd : Context.t -> vhd_record_container -> string -> vhd_record
val remove_lv : Context.t -> vhd_record_container -> pointer -> unit
val get_vhd_hashtbl_copy : Context.t -> vhd_record_container -> (string, vhd_record) Hashtbl.t
val get_vhd_chain : Context.t -> vhd_record_container -> string -> vhd_record list * Lvmabs_types.location_info option
val get_children_from_pointer : Context.t -> vhd_record_container -> pointer -> (string * vhd_record) list
val get_all_affected_vhds : Context.t -> vhd_record_container -> pointer -> string list
val get_vhd_records_rpc : vhd_record_container -> Rpc.t
val get_coalesce_info : Context.t -> vhd_record_container -> (string list * string list * string list * pointer list)

module Tests : sig
	val tests : Ocamltest.test
end
