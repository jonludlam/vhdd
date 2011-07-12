
module type Number = sig
	type t
	val add : t -> t -> t
	val sub : t -> t -> t
	val t_of_rpc : Rpc.t -> t
	val rpc_of_t : t -> Rpc.t
end

module Extentlist (A : Number) =
struct
	type extent = A.t * A.t with rpc
	type t = extent list with rpc

	let ($+) = A.add
	let ($-) = A.sub

	let sort list : t =
		List.sort (fun x y -> compare (fst x) (fst y)) list

	let union list1 list2 =
		let combined = sort (list1 @ list2) in
		let rec inner l acc =
			match l with
				| (s1,e1)::(s2,e2)::ls ->
					let extent1_end = s1 $+ e1 in
					if extent1_end < s2 then
						inner ((s2,e2)::ls) ((s1,e1)::acc)
					else
						let extent2_end = s2 $+ e2 in
						if extent1_end > extent2_end then
							inner ((s1,e1)::ls) acc
						else
							inner ((s1,s2 $+ e2 $- s1)::ls) acc
				| (s1,e1)::[] -> (s1,e1)::acc
				| [] -> []
		in List.rev (inner combined [])

	let intersection list1 list2 =
		let rec inner l1 l2 acc =
			match (l1,l2) with
				| (s1,e1)::l1s , (s2,e2)::l2s ->
					if s1 > s2 then inner l2 l1 acc else
						if s1 $+ e1 < s2 then inner l1s l2 acc else
							if s1 < s2 then inner ((s2,e1 $+ s1 $- s2)::l1s) l2 acc else
								(* s1=s2 *)
								if e1 < e2 then
									inner l1s ((s2 $+ e1,e2 $- e1)::l2s) ((s1,e1)::acc)
								else if e1 > e2 then
									inner ((s1 $+ e2,e1 $- e2)::l1s) l2s ((s2,e2)::acc)
								else (* e1=e2 *)
									inner l1s l2s ((s1,e1)::acc)
				| _ -> List.rev acc
		in
		inner list1 list2 []

	let difference list1 list2 =
		let rec inner l1 l2 acc =
			match (l1,l2) with
				| (s1,e1)::l1s , (s2,e2)::l2s ->
					if s1<s2 then begin
						if s1 $+ e1 > s2 then
							inner ((s2,s1 $+ e1 $- s2)::l1s) l2 ((s1,s2 $- s1)::acc)
						else
							inner l1s l2 ((s1,e1)::acc)
					end else if s1>s2 then begin
						if s2 $+ e2 > s1 then
							inner l1 ((s1,s2 $+ e2 $- s1)::l2s) acc
						else
							inner l1 l2s acc
					end else begin
						(* s1=s2 *)
						if e1 > e2 then
							inner ((s1 $+ e2,e1 $- e2)::l1s) l2s acc
						else if e1 < e2 then
							inner l1s ((s2 $+ e1,e2 $- e1)::l2s) acc
						else
							inner l1s l2s acc
					end
				| l1s, [] -> (List.rev acc) @ l1s
				| [], _ -> List.rev acc
		in
		inner list1 list2 []

	let normalise list =
		let l = sort list in
		let rec inner ls acc =
			match ls with
				| (s1,e1)::(s2,e2)::rest ->
					if s1 $+ e1 > s2 then failwith "Bad list"
					else if s1 $+ e1=s2 then inner ((s1,e1 $+ e2)::rest) acc
					else inner ((s2,e2)::rest) ((s1,e1)::acc)
				| (s1,e1)::[] -> List.rev ((s1,e1)::acc)
				| [] -> List.rev acc
		in
		inner l []
end

(*
module Intextentlist = Extentlist(struct type t=int let add=(+) let sub=(-) end)

open Intextentlist

let test1 = [(1,5);(10,5)]
let test2 = [(0,2);(11,2)]
let test3 = [(4,4);(9,7)]
let test4 = [(0,20)]
*)


