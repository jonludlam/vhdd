(* Filesystem utils *)

let get_total_used_available mountpoint =
	let statvfs = Unixext.statvfs mountpoint in
	let bsize = statvfs.Unixext.f_bsize in
	let blocks = statvfs.Unixext.f_blocks in
	let free = statvfs.Unixext.f_bfree in
	let total = Int64.mul blocks bsize in
	let available = Int64.mul free blocks in
	let used = Int64.sub total available in
	(total,used,available)

