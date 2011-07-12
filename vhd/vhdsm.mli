(** This module is the SMAPI dispatcher *)

(** This module is the top level module that dispatches SMAPI calls to the master or slave
	modules as required. It also calls the transport module to attach and detach the
	underlying block device if necessary.

	It should conform to the Smapi.SMAPI signature, with a few extensions.
*)


(** The SR module *)
module SR :
	sig
		val get_driver_info : Smapi_types.context -> Smapi_types.driver_info
		val delete : Smapi_types.context -> Smapi_types.generic_params -> Smapi_types.sr -> unit 
		val probe :
			Smapi_types.context -> Smapi_types.generic_params -> 'a -> Xml.xml
		val create :
			Smapi_types.context ->
			Smapi_types.generic_params -> Smapi_types.sr -> 'a -> unit
		val scan : Smapi_types.context -> Smapi_types.generic_params -> Smapi_types.sr -> string
		val update : Smapi_types.context -> Smapi_types.generic_params -> Smapi_types.sr -> unit
		val mode : Smapi_types.context -> string -> Int_types.attach_mode
		val maybe_add_pv_info : Smapi_types.context -> Drivers.sm_type -> string -> Smapi_types.sr -> unit
		val reattach : Attachments.sr_info -> unit
		val attach_nomaster : 
			Smapi_types.context ->
			Smapi_types.generic_params -> Smapi_types.sr -> unit
		val attach :
			Smapi_types.context ->
			Smapi_types.generic_params -> Smapi_types.sr -> unit
		val detach :
			Smapi_types.context ->
			Smapi_types.generic_params -> Smapi_types.sr -> unit
		val content_type : 'a -> 'b -> 'c -> string
		val slave_attach : Smapi_types.context -> 'b -> string -> Int_types.host -> (string * bool) list -> string
		val slave_detach : Smapi_types.context -> 'b -> string -> Int_types.host -> unit
		val slave_recover :
			Smapi_types.context -> 'a -> string -> Int_types.host -> unit
		val thin_provision_check : Smapi_types.context -> string -> unit
	end

(** The VDI module *)
module VDI :
	sig
		val create :
			Smapi_types.context ->
			Smapi_types.generic_params ->
			Smapi_types.sr ->
			(string * string) list -> int64 -> Smapi_types.vdi
		val update : Smapi_types.context ->
			Smapi_types.generic_params ->
			Smapi_types.sr -> Smapi_types.vdi -> unit
		val introduce :
			Smapi_types.context ->
			Smapi_types.generic_params ->
			Smapi_types.sr ->
			string -> (string * string) list -> string -> Smapi_types.vdi
		val delete :
			Smapi_types.context ->
			Smapi_types.generic_params -> Smapi_types.sr -> Smapi_types.vdi -> unit
		val snapshot :
			Smapi_types.context ->
			Smapi_types.generic_params ->
			(string * string) list ->
			Smapi_types.sr -> Smapi_types.vdi -> Smapi_types.vdi
		val clone :
			Smapi_types.context ->
			Smapi_types.generic_params ->
			(string * string) list ->
			Smapi_types.sr -> Smapi_types.vdi -> Smapi_types.vdi
		val resize : Smapi_types.context ->
			Smapi_types.generic_params ->
			Smapi_types.sr -> Smapi_types.vdi -> int64 -> Smapi_types.vdi
		val resize_online : Smapi_types.context ->
			Smapi_types.generic_params ->
			Smapi_types.sr -> Smapi_types.vdi -> int64 -> Smapi_types.vdi
		val attach :
			Smapi_types.context ->
			'a -> Smapi_types.sr -> Smapi_types.vdi -> bool -> string
		val detach :
			Smapi_types.context -> 'a -> Smapi_types.sr -> Smapi_types.vdi -> unit
		val activate :
			Smapi_types.context -> 'a -> Smapi_types.sr -> Smapi_types.vdi -> unit
		val deactivate :
			Smapi_types.context -> 'a -> Smapi_types.sr -> Smapi_types.vdi -> unit
		val generate_config : 
			Smapi_types.context -> Smapi_types.generic_params -> Smapi_types.sr -> Smapi_types.vdi -> string
		val leaf_coalesce :
			Smapi_types.context -> 'a -> Smapi_types.sr -> Smapi_types.vdi -> unit
		val slave_attach :
			Smapi_types.context ->
			string -> string -> string -> bool -> bool -> Int_types.slave_attach_info
		val get_slave_attach_info :
			Smapi_types.context ->
			string -> string -> Int_types.slave_attach_info
		val slave_detach :
			Smapi_types.context -> string -> string -> string -> unit
		val slave_activate :
			Smapi_types.context -> string -> string -> string -> bool -> unit
		val slave_deactivate :
			Smapi_types.context -> string -> string -> string -> unit
		val slave_reload : Smapi_types.context -> string -> (string * Int_types.slave_attach_info) list -> unit
		val slave_leaf_coalesce_stop_and_copy :
			Smapi_types.context -> string -> string -> string -> string -> unit
		val external_clone : 'a -> string -> 'b -> 'c
		val slave_set_phys_size :
			Smapi_types.context -> string -> string -> int64 -> unit
		val thin_provision_request_more_space :
			Smapi_types.context ->
			string ->
			string -> (string * 'a * int64) list -> Int_types.dm_node_info list
		val attach_from_config :
			Smapi_types.context ->
			Smapi_types.generic_params ->
			Smapi_types.sr ->
			Smapi_types.vdi -> string -> string
	end

module Host : sig
	val set_dead : Smapi_types.context -> string -> unit
	val rolling_upgrade_finished : Smapi_types.context -> unit
end

(** The Debug module here is an extension to the SMAPI to allow
	communication with the daemon outside of the bounds of the usual
	SMAPI, in order to get internal information for testing. *)
module Debug : sig val vdi_get_leaf_path : Smapi_types.context -> string -> string -> string end
