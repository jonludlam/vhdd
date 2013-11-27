(* Global constants *)

let dummy = ref false
let dummydir = ref "/tmp/mlvm"
let enable_fileserver = ref false
let fileserver_base = ref "/tmp/vhdd_htdocs"
let nodaemon = ref false
let port = ref 4094
let unsafe_mode = ref true
let nowatchdog = ref false
let restart_return_code = 123
let ready = ref false

let host_uuid = ref None
let get_host_uuid () =
	match !host_uuid with
		| None ->
			let id = Inventory.lookup Inventory._installation_uuid in
			host_uuid := Some id;
			id
		| Some id -> id

let mgt_iface = ref None
let get_mgt_iface () =
	match !mgt_iface with
		| None ->
			let iface = Inventory.lookup Inventory._management_interface in
			mgt_iface := Some iface;
			iface
		| Some iface -> iface

let mgt_ip = ref None
let get_mgt_ip () =
        if !dummy then Unix.inet_addr_of_string "127.0.0.1" else
	match !mgt_ip with
		| None -> begin
			let addr = Netdev.Addr.get (get_mgt_iface ()) in
			match addr with
				| (addr,netmask)::_ ->
					mgt_ip := Some addr;
					addr
				| _ -> failwith "Couldn't get IP address"
		end
		| Some addr -> addr

let get_host_local_dummydir () =
	if !dummy then Printf.sprintf "%s/%s" !dummydir (get_host_uuid ()) else ""

let localhost = ref None
let get_localhost () =
	match !localhost with
		| None ->
			let me = {Int_types.h_uuid=get_host_uuid (); h_ip=(try Some (Unix.string_of_inet_addr (get_mgt_ip ())) with _ -> None); h_port= !port} in
			localhost := Some me;
			me
		| Some me -> me

let pool_secret = ref None
let get_pool_secret () =
	match !pool_secret with
		| None ->
			let p = Unixext.string_of_file "/etc/xcp/ptoken" in
			(*pool_secret := Some p; *)
			p
		| Some p -> p

let min_size = ref 1000000000L
let min_size_lock = Mutex.create ()
let pidfile = ref "/var/run/vhdd.pid"
