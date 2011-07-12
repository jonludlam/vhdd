(** This module implements a circular buffer on disk into which data can be reliably written and read concurrently *)

exception No_data
exception Corrupt_data
exception Error_writing
exception Out_of_space
exception Unsupported_version of char

val write : string -> string -> unit
val read : string -> string
val init : string -> int -> unit
