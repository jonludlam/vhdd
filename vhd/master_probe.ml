(* Try to detect the SR master from the attached_hosts location *)
open Stringext
open Int_types
open Context
open Vhd_types

module D=Debug.Make(struct let name="master_probe" end)
open D

exception CorruptHostAttachments
exception CannotFindMaster
exception HostAttachmentsIncorrect
exception NoMaster

let test_master ctx m sr =
	try
	  let client = Int_client.get m in
	  let module Client = (val client : Int_client.CLIENT) in
	  let remote_attach_mode = Client.SR.mode ~sr in
	  begin
	    match remote_attach_mode with
	    | Master -> true
	    | _ -> false
	  end
	with e -> 
		warn "Caught exception while testing the master: %s" (Printexc.to_string e);
		false

let master_probe ctx driver path sr =
	let container =
		match driver with
			| Drivers.Lvm _ ->
				let device = path in
				Lvmabs.init_lvm ctx (String.split ',' device)
			| Drivers.OldLvm _ ->
				let device = path in
				Lvmabs.init_origlvm ctx sr (String.split ',' device)
			| Drivers.File _ ->
				let path = path in
				Lvmabs.init_fs ctx path
	in
	let rec find n =
		try
			match Lvmabs.find_metadata ctx container "host_attachments" with
				| Some (container,location) ->
					let string = Lvmabs.read ctx container location in
					debug "Read string: %s" string;
					let rpc = try Jsonrpc.of_string string with _ -> raise CorruptHostAttachments in
					let s = try Vhd_types.slave_sr_attachment_info_of_rpc rpc with _ -> raise CorruptHostAttachments in
					let master = s.Vhd_types.master in
					let m =
						match master with
							| Some m -> m
							| None -> raise NoMaster
					in
					debug "host_attachments read: Master is apparently at uuid=%s ip=%s port=%d" m.h_uuid (match m.h_ip with Some x -> x | None -> "Unknown") m.h_port;
					if test_master ctx m sr then
						m
					else
						raise HostAttachmentsIncorrect
				| None ->
					debug "No host_attachments metadata found";
					raise CannotFindMaster
		with CorruptHostAttachments as e->
			debug "Caught temporary failure reading host_attachments";
			if n=0 then raise e;
			Thread.delay 1.0;
			find (n-1)
	in

	Pervasiveext.finally
		(fun () -> find 60)
		(fun () -> Lvmabs.shutdown ctx container)
