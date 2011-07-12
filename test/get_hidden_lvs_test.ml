open Vhd

let context = {
	Smapi_types.c_driver = "none";
	c_api_call = "none";
	c_task_id = "none";
	c_other_info = [];
}

let _ = print_endline "Starting test ..."

let container = (Lvmabs.init_lvm context ["/dev/sda3"])

let p lv = print_endline (Lvmabs.string_of_location_info lv)

let _ = List.map p (Lvmabs.get_hidden_lvs context container)

let _ = List.map p (Lvmabs.get_hidden_lvs context container)

(* add test for Lvmabs.add_tag *)
