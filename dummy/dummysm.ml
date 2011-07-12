open Smapi_types

module D = Debug.Debugger(struct let name="dummysm" end)
open D

module SR = struct
  let get_driver_info ctx =
    { di_name = "dummy2";
      di_description = "Daemonized Dummy SR";
      di_vendor = "Ring 3";
      di_copyright = "Citrix";
      di_driver_version = "2";
      di_required_api_version = "1.1";
      di_capabilities = [
	"SR_PROBE";
	"SR_UPDATE";
	"VDI_CREATE";
	"VDI_DELETE";
	"VDI_ATTACH";
	"VDI_DETACH";
	"VDI_RESIZE";
	"VDI_RESIZE_ONLINE";
	"VDI_CLONE";
	"VDI_SNAPSHOT";
	"VDI_ACTIVATE";
	"VDI_DEACTIVATE";
	"VDI_UPDATE";
	"VDI_INTRODUCE";
	"VDI_GENERATE_CONFIG";
	"DAEMONIZE"
      ];
      di_configuration = [];
    }
  let create ctx gp sr size = ()
  let delete ctx gp sr = ()
  let probe ctx gp sr_sm_config = Xml.parse_string "<results/>"
  let scan ctx gp sr = ()
  let update ctx gp sr = ()
  let attach ctx generic_params sr = ()
  let detach ctx generic_params sr = ()
  let content_type ctx generic_params sr = "vhd"
end

let dummy_vdi () = {vdi_info_uuid=Some (Uuid.to_string (Uuid.make_uuid ())); vdi_info_location="/dev/null"}

module VDI = struct
  let create ctx gp sr sm_config size = dummy_vdi ()
  let update ctx gp sr vdi = ()
  let introduce ctx gp sr uuid sm_config location = {vdi_info_uuid=Some uuid; vdi_info_location=location}
  let delete ctx gp sr vdi = ()
  let snapshot ctx gp driver_params sr vdi = dummy_vdi ()
  let clone ctx gp driver_params sr vdi =  dummy_vdi ()
  let resize ctx gp sr vdi newsize =  dummy_vdi ()
  let resize_online ctx gp sr vdi newsize =  dummy_vdi ()

  let attach ctx gp sr vdi = "dummy"
  let detach ctx gp sr vdi = ()
  let activate ctx gp sr vdi = ()
  let deactivate ctx gp sr vdi = ()
  let generate_config ctx gp sr vdi = ""
end  

