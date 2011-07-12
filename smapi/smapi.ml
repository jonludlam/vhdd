open Smapi_types

let unix_socket_path name = Printf.sprintf "/var/xapi/sm/%s" name
let mount_path sr_uuid = Printf.sprintf "/var/run/sr-mount/%s" sr_uuid

module type SMAPI = sig
  module SR : sig
      (** The calls 'probe' 'scan' 'update' 'create' and 'delete'
	  are only called by xapi on the designated SR master *)
    val probe : context -> generic_params -> (string * string) list -> Xml.xml
    val scan : context -> generic_params -> sr -> string
    val update : context -> generic_params -> sr -> unit
    val create : context -> generic_params -> sr -> int64 -> unit
    val delete : context -> generic_params -> sr -> unit
      
    (** Attach, detach and content_type can be called on masters or slaves *)
    val get_driver_info : context -> driver_info
    val attach : context -> generic_params -> sr -> unit
    val detach : context -> generic_params -> sr -> unit
    val content_type : context -> generic_params -> sr -> string
  end
    
  module VDI : sig
      (** The following calls are only called on the SR master *)
    val create : context -> generic_params -> sr -> (string * string) list -> int64 -> vdi
    val update : context -> generic_params -> sr -> vdi -> unit
    val introduce : context -> generic_params -> sr -> string -> (string * string) list -> string -> vdi
    val delete : context -> generic_params -> sr -> vdi -> unit
    val snapshot : context -> generic_params -> (string * string) list -> sr -> vdi -> vdi
    val clone : context -> generic_params -> (string * string) list -> sr -> vdi -> vdi
    val resize : context -> generic_params -> sr -> vdi -> int64 -> vdi
    val resize_online : context -> generic_params -> sr -> vdi -> int64 -> vdi
      
    (** Xapi calls the following functions on masters and slaves *)
    val attach : context -> generic_params -> sr -> vdi -> bool -> string
    val detach : context -> generic_params -> sr -> vdi -> unit
    val activate : context -> generic_params -> sr -> vdi -> unit
    val deactivate : context -> generic_params -> sr -> vdi -> unit
    val generate_config : context -> generic_params -> sr -> vdi -> string
	val attach_from_config : context -> generic_params -> sr -> vdi -> string -> string
  end
end

