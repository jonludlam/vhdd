(** Responsible for keeping track of which SRs have been attached on this host.

	log_attachment/log_detachment/log_attachment_new_master write to the hosts
	disk to make sure that in the event of the daemon's death we can recover
	our state.

	read_attachments is used to reattach the previously attached SRs

	Also keeps track of the metadata for each SR. Master metadata is kept
	separately from the slave metadata. These are added/removed by the
	attach_as_{master|slave} and detach_as_{master|slave}.

	It is safe to call the functions in this module in parallel.
*)

(** A type expressing the information stored on disk about an attached SR *)
type sr_info = {
  drivertype : string; (* see drivers.ml for a list *)
  path : string; (* path for the transport layer - either a path to a filesystem or block device *)
  uuid : string; 
  mode : Int_types.attach_mode; (* master/slave *)
  device_config : (string * string) list; (* device config that was used in the sr-attach call *)
}

(** Read the attachments from disk *)
val read_attachments : unit -> sr_info list

(** Log the fact that we have attached *)
val log_attachment : sr_info -> unit

(** Log the fact that we've detached *)
val log_detachment : string -> unit

(** Change an attachment to point to a new master *)
val log_attachment_new_master : string -> Int_types.host option -> unit

(** Put the master metadata into the global hashtbl *)
val attach_as_master : string -> Vhd_types.master_sr_metadata -> unit

(** Put the slave metadata into the global hashtbl *)
val attach_as_slave : string -> Vhd_types.slave_sr_metadata -> unit

(** Remove the master metadata from the global hashtbl *)
val detach_as_master : string -> unit

(** Remove the slave metadata from the global hashtbl *)
val detach_as_slave : string -> unit

(** Retrieve the master metadata *)
val gmm : Vhd_types.sr -> Vhd_types.master_sr_metadata

(** Retrieve the slave metadata *)
val gsm : Vhd_types.sr -> Vhd_types.slave_sr_metadata

(** Map a function over the master metadatas *)
val map_master_srs :
  (string -> Vhd_types.master_sr_metadata -> 'a) -> 'a list

(** Map a function over the slave metadatas *)
val map_slave_srs :
  (string -> Vhd_types.slave_sr_metadata -> 'a) -> 'a list
