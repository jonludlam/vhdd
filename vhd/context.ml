type t = {
  c_driver : string;
  c_api_call : string;
  c_task_id : string;
  mutable c_other_info : (string * string) list
} with rpc

