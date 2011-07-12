(* Read/write lock, with fair queuing *)

(* isn't implemented yet! *)

type t = Mutex.t

let create () =
	Mutex.create ()

let read_lock t =
	Mutex.lock t

let write_lock t =
	Mutex.lock t

let read_unlock t =
	Mutex.unlock t

let write_unlock t =
	Mutex.unlock t

let with_read_lock t f =
	read_lock t;
	Pervasiveext.finally f (fun () -> read_unlock t)

let with_write_lock t f =
	write_lock t;
	Pervasiveext.finally f (fun () -> write_unlock t)
