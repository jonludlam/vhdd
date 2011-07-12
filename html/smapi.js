/* SMAPI calls */

var backends = ["lvm","lvmnew","lvmoiscsi","lvmnewiscsi","lvmohba","lvmnewhba","ext","nfs"];
var apis={};

function gen_smapi_rpc(i,backend)
{
  apis[backend]=new $.rpc(
	"/fd_dispatcher/vhdd/debug?path="+backend,
    "xml",
	function () {},
	null,
	["sr_attach","sr_detach","sr_probe","sr_delete","sr_create","sr_get_driver_info","sr_content_type","sr_scan","sr_update","vdi_attach","vdi_detach","vdi_activate","vdi_deactivate","vdi_generate_config","vdi_attach_from_config","vdi_create","vdi_update","vdi_introduce","vdi_delete","vdi_snapshot","vdi_clone","vdi_resize","vdi_resize_online"]
  );
}



jQuery.each(backends,gen_smapi_rpc);
