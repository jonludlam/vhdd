type container_info =
	| VolumeGroup of Lvm.Vg.vg
	| FileSystem of string (* path *)
	| OrigLVMVG of Olvm.vg (* VG name *)
	with rpc

type location_type =
	| Vhd | Raw | Metadata

and location =
	| LogicalVolume of string (* LV name *)
	| OLV of Olvm.lv
	| File of string

and location_info = {
	location : location;
	location_type : location_type;
} with rpc

