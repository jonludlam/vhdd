open Int_types

type vdi = Storage_interface.vdi
type sr = Storage_interface.sr

type lock = string

module SR = struct
  external slave_attach :  sr:sr -> host:host -> vdis:(vdi * bool) list -> string = ""

  external slave_detach :  sr:sr -> host:host -> unit = ""

  external slave_recover :  sr:sr -> master:host -> unit = ""

  external mode :  sr:sr -> attach_mode = ""

  external thin_provision_check :  sr:sr -> unit = ""
    
end

module VDI = struct

  external slave_attach : host_uuid:string -> sr:sr -> vdi:vdi -> writable:bool -> is_reattach:bool -> slave_attach_info = ""

  external get_slave_attach_info :  sr:sr -> vdi:vdi -> slave_attach_info = ""

  external slave_detach : host_uuid:string -> sr:sr -> vdi:vdi -> unit = ""
    
  external slave_activate : host_uuid:string -> sr:sr -> vdi:vdi -> is_reactivate:bool -> unit = ""
    
  external slave_deactivate : host_uuid:string -> sr:sr -> vdi:vdi -> unit = ""



  external slave_reload : sr:sr -> vdis:(vdi * slave_attach_info) list -> unit = ""

  external slave_leaf_coalesce_stop_and_copy : sr:sr -> vdi:vdi -> leaf_path:string -> new_leaf_path:string -> unit = ""
    
  external slave_set_phys_size :  sr:sr -> vdi:vdi -> size:int64 -> unit= ""
    
  external thin_provision_request_more_space :  sr:sr -> host_uuid:string -> sizes:vdi_size_data list -> dm_node_info_list = ""

end

module Debug = struct
  external waiting_locks_get : unit -> (lock * lcontext) list = ""

  external waiting_lock_unwait :  lock:lock -> unit = ""

  external waiting_mode_set :  mode:bool -> unit = ""

  external vdi_get_leaf_path :  sr:sr -> vdi:vdi -> string = ""

  external die :  restart:bool -> unit = ""
    
  external get_id_to_leaf_map :  sr:sr -> Vhd_types.id_to_leaf_mapping_t = ""

  external get_vhds :  sr:sr -> Vhd_records.vhd_record_container = ""
    
  external get_master_metadata :  sr:sr -> Vhd_types.master_sr_metadata_data = ""

  external get_slave_metadata :  sr:sr -> Vhd_types.slave_sr_metadata_data = ""
    
  external get_attached_vdis :  sr:sr -> Vhd_types.attached_vdis_t = ""

  external get_vhd_container :  sr:sr -> Lvmabs_types.container_info = ""

  external get_pid :  unit -> int = ""

  external get_attach_finished :  sr:sr -> bool = ""

  external slave_get_leaf_vhduid :  sr:sr -> vdi:vdi -> string = ""

  external write_junk :  sr:sr -> vdi:vdi -> size:int64 -> n:int -> current:Junk.t -> Junk.t = ""

  external check_junk :  sr:sr -> vdi:vdi -> current:Junk.t -> unit = ""

  external get_host : unit -> string = ""

  external get_ready : unit -> bool = ""
end



