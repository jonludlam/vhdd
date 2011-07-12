$(document).ready(function() { load_tracelog(); });

var id_n=1;
var vhd_n=1;
var id_map = {};
var vhd_map = {};

function get_or_create_id_map(id)
{
  if(!id_map[id]) {
	id_map[id]="vdi_"+id_n;
	id_n++;
  }
  return(id_map[id]);
}

function get_or_create_vhd_map(id)
{
  if(!vhd_map[id]) {
	vhd_map[id]="vhd_"+vhd_n;
	vhd_n++;
  }
  return(vhd_map[id]);
}

function get_all_task_ids() {
  var task_ids={
  };

  for(var i=0; i<global_tracelog.log.length-1; i++) {
	task_ids[global_tracelog.log[i].task_id]=true;
  }

  return task_ids;
}

function get_all_threads() {
  var threads={};

  for(var i=0; i<global_tracelog.log.length-1; i++) {
	task_ids[global_tracelog.log[i].thread_id]=true;
  }

  return threads;
}

function follow_tracelog(handler) {
  var state={locks:{s_mutex:-1, m_attached_hosts_lock:-1, m_coalesce_in_progress_lock:false, m_container_lock:-1, m_id_mapping_lock:-1, m_vhd_hashtbl_lock:-1 } };

  for(var i=global_tracelog.log.length - 1; i>=0; i--) {
	var event=global_tracelog.log[i];

	switch(event.data[0]) {
	case "Master_m_vhd_container":
	  state.m_vhd_container = event.data[1];
	  break;
	case "Master_m_id_to_leaf_mapping":
	  state.m_id_to_leaf_mapping = event.data[1];
	  break;
	case "Master_m_vhds":
	  state.m_vhds = event.data[1];
	  break;
	case "Master_m_attached_hosts":
	  state.m_attached_hosts = event.data[1];
	  break;
	case "Master_m_coalesce_in_progress":
	  state.locks.m_coalesce_in_progress_lock = event.data[1];
	  break;
	case "Master_m_rolling_upgrade":
	  state.m_rolling_upgrade = event.data[1];
	  break;
	case "Master_id_to_leaf_update":
	  state.m_id_to_leaf_mapping[event.data[1]]=event.data[2];
	  break;
	case "Master_id_to_leaf_add":
	  state.m_id_to_leaf_mapping[event.data[1]]=event.data[2];
	  break;
	case "Master_m_id_to_leaf_remove":
	  delete state.m_id_to_leaf_mapping[event.data[1]];
	  break;
	case "LockTaken":
	  state.locks[event.data[1]]=event.thread_id;
	  break;
	case "LockReleased":
	  state.locks[event.data[1]]=(-1);
	  break;

	case "Slave_s_master":
	  state.s_master=event.data[1];
	  break;
	case "Slave_s_current_ops":
	  state.s_current_ops=event.data[1];
	  break;
	case "Slave_s_master_approved_ops":
	  state.s_master_approved_ops=event.data[1];
	  break;
	case "Slave_s_attached_vdis":
	  state.s_attached_vdis=event.data[1];
	  break;
	case "Slave_s_ready":
	  state.s_ready=event.data[1];
	  break;
	case "Slave_s_thin_provision_request_in_progress":
	  state.s_thin_provision_request_in_progress=event.data[1];
	  break;
	case "Slave_s_current_ops_add":
	  state.s_current_ops[event.data[1][0]]=event.data[1][1];
	  break;
	case "Slave_s_current_ops_remove":
	  delete state.s_current_ops[event.data[1]];
	  break;
	case "Slave_s_master_approved_ops_add":
	  state.s_master_approved_ops[event.data[1][0]]=event.data[1][1];
	  break;
	case "Slave_s_master_approved_ops_remove":
	  delete state.s_master_approved_ops[event.data[1]];
	  break;
	case "Slave_s_attached_vdis_add":
	  state.s_attached_vdis[event.data[1]]=event.data[2];
	  break;
	case "Slave_s_attached_vdis_remove":
	  delete state.s_attached_vdis[event.data[1]];
	  break;
	case "Slave_s_attached_vdis_update":
	  state.s_attached_vdis[event.data[1]]=event.data[2];
	  break;
	default:
	  console.log("Unhandled tracelog entry: "+event.data[0]);
	  break;
    }

	handler(state,event,global_tracelog.log.length - 1 - i);
  }
}
function transform_value(json)
{
  if(typeof(json)=="string")
	return json;
  if(typeof(json)=="object") {
	if(json.hasOwnProperty("array")) {
	  return []; // TODO
	 }
	 if(json.hasOwnProperty("struct")) {
		  return transform_struct(json.struct);
	 }

  }
}

function transform_struct(json)
{
  var result={};

  if(typeof(json)=="string") {
	return result;
  }
  for(var i=0; i<json.member.length; i++) {
	result[json.member[i].name]=transform_value(json.member[i].value);
  }
  return result;
}

function get_with_filter(filter,handler)
{
  function fhandler(state,event,n) {
	if(filter(state,event,n)) {
	  handler(state,event,n);
	}
  }
  follow_tracelog(fhandler);
}

function get_state_at(num) {
  function handler(state,event,n) {
	if(num==n) {
	  state_at = state;
	}
  }

  follow_tracelog(handler);
}

function findsmapicall(task_id)
{
  var startevent, endevent;
  var vdis={};
  for(var i=0; i<global_tracelog.log.length; i++) {
	var event=global_tracelog.log[i];
	if(event.task_id==task_id) {
	  if(event.data[0]=="SmapiCall") {
		startevent=event;
	  } else if(event.data[0]=="SmapiResult") {
		endevent=event;
	  }
	  if(event.other_info.vdi)
		vdis[event.other_info.vdi]=true;
	}

  }

  if(!startevent || !endevent)
	return {};

  var jsonsmapiresult=$.xml2json(endevent.data[1].result);
  var orig_call=$.xml2json(startevent.data[1].body);
  var orig_call2=transform_value(orig_call.params.param.value);

  var result = {
	call:startevent.data[1].call,
	time:endevent.timestamp-startevent.timestamp,
	result:jsonsmapiresult,
	starttime:startevent.timestamp,
	succeeded:(jsonsmapiresult.hasOwnProperty("params")),
	call_args:orig_call2,
	vdis:vdis
  };


  return result;
}

function test() {
  var tasks = get_all_task_ids();

  var tasks2=[];
  var i=0;
  for(var key in tasks)
	tasks2[i++]=key;

  var mytask = tasks2[Math.floor(Math.random()*tasks2.length)];

  function myfilter(state,event,n) {
	return (event.task_id==mytask);
  }

  function handler(state,event,n) {
    if(event.message)
	  console.log(event.message);
  }

  get_with_filter(myfilter,handler);
}

function do_task_list_table() {

  var taskresult={};
  var tasks_obj=get_all_task_ids();
  var tasks=[];
  var i=0;
  for(task_id in tasks_obj) {
	tasks[i++]=task_id;
	taskresult[task_id]=findsmapicall(task_id);
  }

  var rows = [];

  tasks.sort(function(a,b) {
			   if(taskresult[a].starttime<taskresult[b].starttime) {
				 return -1;
			   } else {
				 return 1;
			   }
			 });
  console.log(tasks[0]);
  var starttime=taskresult[tasks[0]].starttime;

  for(var i=0; i<tasks.length; i++) {
	var task_id=tasks[i];
	rows=rows.concat(["tr",{},["td",{},["a",{id:"task_id_"+tasks[i],href:"#"},[tasks[i]]],
					  "td",{},[(taskresult[task_id].starttime-starttime)+""],
					  "td",{},[taskresult[task_id].call+""],
					  "td",{},[taskresult[task_id].time+""],
					  "td",{},[taskresult[task_id].succeeded+""]
					 ]]);
  }

  var table = ["table",{},["thead",{},["tr",{},["th",{},["task_id"],
												"th",{},["start time"],
												"th",{},["call"],
												"th",{},["time"],
												"th",{},["succeeded"]]],
						   "tbody",{},rows]];

  $('#tables').empty().append($($.create.apply($,table)));

  for(i=0; i<tasks.length; i++) {
	var task_id=tasks[i];
	$('#task_id_'+task_id).click((function(mytask) {
								   return function() {
									 trace_task(mytask);
								   };})(task_id));
  }
}

function call_name_from_xmlrpc(xmlrpc)
{
  var xmlrpccall=$.xml2json(xmlrpc);
  return xmlrpccall.methodName;
}

function trace_task(task_id)
{
// columns:
// locks: id2leaf,hashtbl,

  var taskresult=findsmapicall(task_id);
  var inittimestamp;
  var columns=[
	{
	  general:[{time:function(state,event,n) {
		if(!inittimestamp)
		  inittimestamp=event.timestamp;
		  return(event.timestamp-inittimestamp);
				}},
			   {
				 thread_id:function(state,event,n) {
				   return event.thread_id;
				 }
			   }
			  ]
	},
	{master:[{
			   locks:[
				 {AH: function(state,event,n) {
				   if(state.locks.m_attached_hosts_lock==event.thread_id) {
					 return { attr:{ "class":"locked"}, txt:" "};
				   } else {
					 return {attr:{}, txt:" "};
				   }}},
				 {IM: function(state,event,n) {
					if(state.locks.m_id_mapping_lock==event.thread_id) {
					  return { attr:{ "class":"locked"}, txt:" "};
					} else {
					  return {attr:{}, txt:" "};
					}}},
				 {VH: function(state,event,n) {
					if(state.locks.m_vhd_hashtbl_lock==event.thread_id) {
					  return { attr:{ "class":"locked"}, txt:" "};
					} else {
					  return {attr:{}, txt:" "};
					}}},
				 {CP: function(state,event,n) {
					if(state.locks.m_coalesce_in_progress_lock) {
					  return { attr:{ "class":"locked"}, txt:" "};
					} else {
					  return {attr:{}, txt:" "};
					}}},
			   ]
			 },
			 {message:function(state,event,n) {
				if(event.message && event.other_info.module && event.other_info.module=="vhdMaster")
				  return(event.message);
				else
				  return "";}}]},
	{calls:function(state,event,n) {
				if(event.data[0]=="Master_to_slave_call") {
				  var call_name = event.data[1][0];
				  return "->->->->->"+call_name+"->->->->->";
				} else if(event.data[0]=="Master_to_slave_response") {
				  var txt=event.data[1][0];
				  return "<-<-<-<-<-"+txt+"<-<-<-<-<-";
				} else if(event.data[0]=="Slave_to_master_call") {
				  var call_name = event.data[1][0]
				  return "<~~~~~~~~~"+call_name+"<~~~~~~~~~";
				} else if(event.data[0]=="Slave_to_master_response") {
				  var txt=event.data[1][0];
				  return "~~~~~~~~~>"+txt+"~~~~~~~~~>";
				} else {
				  return "";
				}
			  }
	},

	{slave:[{
			  locks:[{M:function(state,event,n) {
				if(state.locks.s_mutex==event.thread_id) {
				  return { attr:{ "class":"locked"}, txt:" "};
				} else {
				  return {attr:{}, txt:" "};
				}}}]},
			{
			  message:function(state,event,n) {
				if(event.message && event.other_info.module && event.other_info.module=="vhdSlave")
				  return(event.message);
				else
				  return "";}}]}
  ];

  var master_vdi_locks=[];
  var slave_vdi_locks=[];
  for(vdi in taskresult.vdis) {
	mvl={};
	mvl[get_or_create_id_map(vdi)]=(function(curvdi) {return (function(state,event,n) {
							if(state.m_id_to_leaf_mapping[curvdi].current_operation) {
							  return {
								attr:{
								  "class":"locked"
								},
								txt:state.m_id_to_leaf_mapping[curvdi].current_operation
							  };
							} else return "";
							  });})(vdi);
	master_vdi_locks = master_vdi_locks.concat(mvl);
	svl={};
	svl["CO("+get_or_create_id_map(vdi)+")"]=(function(curvdi) {
				return (function(state,event,n) {
							if(state.s_current_ops[curvdi]) {
							  return {
								attr:{
								  "class":"locked"
								},
								txt:state.s_current_ops[curvdi]
							  };
							} else return "";
						});})(vdi);
	svl2={};
	svl2["MAO("+get_or_create_id_map(vdi)+")"]=(function(curvdi) {
				return (function(state,event,n) {
						  if(state.s_master_approved_ops[curvdi]) {
							  return {
								attr:{
								  "class":"locked"
								},
								txt:state.s_master_approved_ops[curvdi]
							  };
							} else return "";

						  return (state.s_master_approved_ops[curvdi] || "");
						});})(vdi);
	columns[1].master[0].locks=columns[1].master[0].locks.concat(mvl);
	columns[3].slave[0].locks=columns[3].slave[0].locks.concat(svl);
	columns[3].slave[0].locks=columns[3].slave[0].locks.concat(svl2);
  }

  function get_max_depth(columns) {
	var max=1;
	var foo=typeof(columns);
	if(!columns.hasOwnProperty(length))
	  return max;
	for(var i=0; i<columns.length; i++) {
	  for(var subcol in columns[i]) {
		var subcoldepth=get_max_depth(columns[i][subcol])+1;
		if(subcoldepth > max)
		  max=subcoldepth;
	  }
	}
	return max;
  }

  function get_num_subcols(column) {
	if(!column.hasOwnProperty(length)) {
	  return 1;
    } else {
	  var colcount=0;
	  for(var i=0; i<column.length; i++) {
		for(var subcol in column[i])
		  colcount+=get_num_subcols(column[i][subcol]);
	  }
	  return colcount;
	}
  }

  var maxdepthleft = get_max_depth(columns);

  var thead=[];
  function do_table_header(columns,depthleft) {
	var nextrow = [];
	var tr=[];
	for(var i=0; i<columns.length; i++) {
	  for(var col in columns[i]) {
		var subcols = get_num_subcols(columns[i][col]);
		if(typeof(columns[i][col]=="array"))
		  nextrow=nextrow.concat(columns[i][col]);
		var attr={};
		if(subcols>1)
		  attr['colspan']=subcols;
		var mymaxdepth=get_max_depth(columns[i][col]);
		if(mymaxdepth==1)
		  attr['rowspan']=(depthleft-mymaxdepth);
		tr=tr.concat(["th",attr,[col]]);
	  }
	}
	thead=thead.concat(["tr",{},tr]);
	if(nextrow.length>0)
	  do_table_header(nextrow,depthleft-1);
  }

  do_table_header(columns,maxdepthleft);



  function myfilter(state,event,n) {
	return (event.task_id==task_id);
  }

  var tbody=[];

  $('#testtable').empty().append($($.create('table',{},["thead",{},thead,"tbody",{id:"mytbody"},[]])));

  var mytbody=$('#mytbody');
  function handler(state,event,n) {
	var row=[];

	function doit(columns) {
	  for(var i=0; i<columns.length; i++) {
		for(var col in columns[i]) {
		  var fn=columns[i][col];
		  if(fn.constructor == Array) {
			doit(columns[i][col]);
		  } else {
			var result=columns[i][col](state,event,n);
			var attr={};
			var txt="";
			if(result.hasOwnProperty("attr")) {
			  attr=result.attr;
			  txt=result.txt;
			} else {
			  txt=result+"";
			}
			row=row.concat(["td",attr,[txt]]);
		  }
		}
	  }
	}
	doit(columns);
	var tr=$($.create("tr",{},row));
	tr.click(function(myevent,state,n) {
			   return function() {
			     console.log(n);
				 console.log(myevent.timestamp);
				 console.log(myevent.data[0]);
				 console.log(myevent.data[1] || "No data[1]");
				 console.log(myevent.message || "No message");
				 globalstate=state;
				 for(lock in state.locks) {
				 		  console.log(lock+": "+state.locks[lock]);
				 		  }

			   };
			 } (event,state,n));
	mytbody.append(tr);
  }

  get_with_filter(myfilter,handler);



}

function handle_tracelog(tracelog)
{
  global_tracelog=tracelog;
  $('#tables').empty().append($($.create.apply($,do_task_list_table())));
}

function load_tracelog()
{
    $.getJSON("/fd_dispatcher/vhdd/tracelog",{},handle_tracelog);
}