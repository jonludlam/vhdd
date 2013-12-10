module I=struct
	type t=int64 with rpc
	let add=Int64.add
	let sub=Int64.sub
end

module Int64extentlist = Extentlist.Extentlist(I)

type t = (Int64extentlist.t * char) list with rpc
