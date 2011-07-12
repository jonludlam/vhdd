var oddrow = true;

function vhd_to_tr(k, v) {
    var p="none";

    if(v.hasOwnProperty("parent"))
	p=v.parent[0]+"("+v.parent[1]+")";

    oddrow = !oddrow;

    var attr = {};
    if(oddrow) {
	attr['class']="oddrow";
    }
    return ["tr",attr,["td",{},[k],
		       "td",{},[""+v.size.phys_size],
		       "td",{},[""+v.size.critical_size],
		       "td",{},[p],
		       "td",{},[v.location.location[1]],
		       "td",{},[""+v.hidden]]];
}

function id_map_to_tr(k, v) {
		oddrow = !oddrow;
		var attr = {};
		if(oddrow) {
				attr['class']="oddrow";
		}
		var current_op = "None";
		if(v.hasOwnProperty("current_operations")) {
			current_op=v.current_operations.cur.join(",");
		}
		var attachment = "None";
		if(v.hasOwnProperty("attachment")) {
				attachment=v.attachment[0]+"(";
				for(var i=0; i<v.attachment[1].length; i++) {
						attachment+=v.attachment[1][i];
						if(i<v.attachment[1].length-1)
								attachment+=",";
				}
				attachment+=")";
		}
		var activations = "None";
		if(v.hasOwnProperty("active_on")) {
				activations=v.active_on[0]+"(";
				if(typeof(v.active_on[1])=="string") {
						activations+=v.active_on[1];
				} else {
						for(var i=0; i<v.active_on[1].length; i++) {
								activations+=v.active_on[1][i];
								if(i<v.active_on[1].length-1)
										activations+=",";
						}
				}
				activations+=")";

		}
		var leaf = v.leaf[0]+"("+v.leaf[1]+")";
		return ["tr",attr,["td",{},[k],
						   "td",{},[current_op],
						   "td",{},[attachment],
						   "td",{},[activations],
						   "td",{},[leaf]]];
}

function host_to_tr(k,v) {
    var attr = {};
    if(oddrow) {
	attr['class']="oddrow";
    }
    return ["tr",attr,["td",{},[v.ssa_host.h_uuid],
		       "td",{},[v.ssa_host.h_ip],
		       "td",{},[""+v.ssa_resync_required]]];
}

function master_to_html(metadata) {
    var vhd_rows=[];

    for(vhd in metadata.m_vhds.hashtbl) {
	vhd_rows=vhd_rows.concat(vhd_to_tr(vhd,metadata.m_vhds.hashtbl[vhd]));
    }

		var id_rows=[];

		for(id in metadata.m_id_to_leaf_mapping) {
				id_rows=id_rows.concat(id_map_to_tr(id,metadata.m_id_to_leaf_mapping[id]));
		}

    var host_rows=[];

    for(host in metadata.m_attached_hosts) {
	host_rows=host_rows.concat(host_to_tr(host,metadata.m_attached_hosts[host]));
    }



		return ["div",{},
				["span",{},
				 ["SR UUID: "+metadata.m_sr_uuid],
				 // master VHDs
				 "div",{"class":"master_vhds"},
				 ["h2",{},["VHDs"],
				  "table",{},
				  ["thead",{},
				   ["tr",{},
					["th",{},["vhduid"],
					 "th",{},["phys_size"],
					 "th",{},["critical_size"],
					 "th",{},["parent"],
					 "th",{},["location"],
					 "th",{},["hidden"]
					]
				   ],
				   "tbody",{},vhd_rows
				  ]
				 ],
				 // master attached hosts
				 "div",{"class":"master_attached_hosts"},
				 ["h2",{},["Connected hosts"],
				  "table",{},
				  ["thead",{},
				   ["tr",{},
					["th",{},["uuid"],
					 "th",{},["ip"],
					 "th",{},["needs resync"]
					]
				   ],
				   "tbody",{},host_rows
				  ]
				 ],
				 // master leaf info
				 "div",{"class":"master_leaf_info"},
				 ["h2",{},["Leaf info"],
				  "table",{},
				  ["thead",{},
				   ["tr",{},
					["th",{},["id"],
					 "th",{},["current operations"],
					 "th",{},["attachments"],
					 "th",{},["activations"],
					 "th",{},["leaf uid"]
					]
				   ],
				   "tbody",{},id_rows
				  ]
				 ]
				]
			   ];
}

function s_vdi_to_tr(k,v)
{
    oddrow = !oddrow;

    var attr = {};
    if(oddrow) {
	attr['class']="oddrow";
    }

	var lvs = [];
	for(var lv=0; lv<v.savi_attach_info.sa_lvs.length; lv++) {
	  lvs=lvs.concat(["ul",{},[v.savi_attach_info.sa_lvs[lv][1].dmn_dm_name]]);
	}

	var actions = ["button",{id:"detach_"+k},["detach"]];
	if(v.savi_activated) {
	  actions=actions.concat(["button",{id:"deactivate_"+k},["deactivate"]]);
	}

	return ["tr",attr,
			["td",{},[k],
			 "td",{},["("+v.savi_blktap2_dev.minor+","+v.savi_blktap2_dev.tapdisk_pid+")"],
			 "td",{},[v.savi_attach_info.sa_leaf_path],
			 "td",{},lvs,
			 "td",{},[""+v.savi_phys_size],
			 "td",{},[""+v.savi_maxsize],
			 "td",{},[""+v.savi_activated],
			 "td",{},["div",{},actions]
			]];
}


function slave_to_html(metadata) {
		var sr_uuid = "SR UUID: "+metadata.s_sr.sr_uuid;
		if(metadata.s_ready)
				sr_uuid += " (ready)";
		else
				sr_uuid += " (not ready)";

		currently_attached_vdis=[];
		for(vdi in metadata.s_attached_vdis) {
				currently_attached_vdis=currently_attached_vdis.concat(s_vdi_to_tr(vdi,metadata.s_attached_vdis[vdi]));
		}

		return ["div",{},
				["span",{},
				 [sr_uuid,
				  "h2",{},["Current master"],
				  "table",{},
				  ["thead",{},
				   ["tr",{},
					["th",{},["uuid"],
					 "th",{},["ip"],
					 "th",{},["port"]
					],
				   ],
				   "tbody",{},
				   ["tr",{},
					["td",{},[metadata.s_master.h_uuid],
					 "td",{},[metadata.s_master.h_ip],
					 "td",{},[""+metadata.s_master.h_port]
					]
				   ]
				  ],
				  "h2",{},["Currently attached VDIs"],
				  "table",{},
				  ["thead",{},
				   ["tr",{},
					["th",{},["Location"],
					 "th",{},["tapdev"],
					 "th",{},["leaf"],
					 "th",{},["LVs"],
					 "th",{},["phys_size"],
					 "th",{},["max size"],
					 "th",{},["activated"],
					 "th",{},["actions"]
					]
				   ],
				   "tbody",{},currently_attached_vdis
				  ]
				 ]
				]
			   ];
}
function add_actions(metadata)
{
  for(vdi in metadata.s_attached_vdis) {
	$('#detach_'+vdi).click((function(myvdi) {
							  return function() {
								apis.lvm.vdi_detach({sr_uuid:metadata.s_sr.sr_uuid, device_config:{}, vdi_location:myvdi, vdi_uuid:""});
							  };
							 })(vdi));
	$('#deactivate_'+vdi).click((function(myvdi) {
							   return function() {
								 apis.lvm.vdi_deactivate({sr_uuid:metadata.s_sr.sr_uuid, device_config:{}, vdi_location:myvdi, vdi_uuid:""});
							   };
								 })(vdi));
  }
}
var current_update=0;

function handle_updates(data) {
		current_update=data.id;
		current_data=data;
		if(data.master_metadatas[0]) {
				$('#attached_as_master').empty().append($($.create.apply($,master_to_html(data.master_metadatas[0]))));
		}
		if(data.slave_metadatas[0]) {
				$('#attached_as_slave').empty().append($($.create.apply($,slave_to_html(data.slave_metadatas[0]))));
				  add_actions(data.slave_metadatas[0]);
		}
		start_update();
}



/* Need to figure out whether we're being served by vhdd or xapi 
 *
 * if xapi, we use a longer uri for the updates.
 */

var vhdduri = "updates";
var xapiuri = "/fd_dispatcher/vhdd/updates";
var usevhdduri = true;
var vhddurisuccess = false;

function error() {
		 if(usevhdduri) {
		    console && console.log("Caught error in updates: falling back to xapi uri");
		 	usevhdduri=false;
			start_update();
		 } else {
		    console && console.log("Caught error in updates: falling back to vhdd uri");
			usevhdduri=true;
			start_update();
		 }		 		
}


function start_update() {
	var uri=xapiuri;
	if(usevhdduri) {
		uri=vhdduri;
	}
//    $.getJSON(uri,{id:current_update},handle_updates);
	$.ajax({url:uri,
			dataType:"json",
			data:{id:current_update},
            success:handle_updates,
            error:error});
}

$(document).ready(function () {start_update(0)})

