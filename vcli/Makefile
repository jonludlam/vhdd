OCAMLC = ocamlfind ocamlc -g
OCAMLOPT = ocamlfind ocamlopt -g
OCAMLPACKS = str,xapi-client,xmlm,xml-light2,stdext,uuid,camldm,lvm,rpc-light.syntax,rpc-light.json,netdev,threads,vhd,tapctl
COMPFLAG = -thread -package $(OCAMLPACKS) -syntax camlp4o -I ../smapi -dtypes
LINKFLAG = -linkpkg ../smapi/smapi.cmxa
SOURCES = \
	ocamltest \
	circ \
	olvm \
	int_types \
	nmutex \
	lockgen \
	host \
	fsutils \
	lvmabs_types \
	xapi_inventory \
	global \
	vhdutil \
	tapdisk \
	vhd_records \
	vhd_types \
	tracelog \
	lvmabs \
	scan \
	errors \
	drivers \
	rwlock \
	vhdrpc \
	extentlist \
	int_rpc \
	nfs \
	iscsilib \
	transport \
	attachments \
	int_client \
	dot \
	html \
	slave_sr_attachments \
	locking \
	xapi \
	int_client_utils \
	tapdisk_listen \
	id_map \
	master_utils \
	coalesce \
	master_probe \
	clone \
	leaf_coalesce \
	vhdSlave \
	vhdMaster \
	fd_pass_receiver \
	vhdsm \
	debug_utils \
	int_server \
	fileserver 

OBJS = $(foreach obj, $(SOURCES),../vhd/$(obj).cmx)
MLS = $(foreach obj, $(SOURCES),../vhd/$(obj).ml)

default: vcli attach_from_config_cli

vcli : vcli.ml $(MLS)
	ocamlfind ocamlopt -package $(OCAMLPACKS) \
	  	-syntax camlp4o -dtypes -thread -linkpkg -o vcli -I ../smapi ../smapi/smapi.cmxa -I ../vhd \
	 	$(OBJS) vcli.ml

attach_from_config_cli : attach_from_config_cli.ml
	ocamlfind ocamlopt -package stdext,xapi-client,threads,rpc-light.syntax,rpc-light,camldm \
	-dtypes -thread -linkpkg -o attach_from_config_cli -I ../smapi ../smapi/smapi.cmxa \
	attach_from_config_cli.ml 

.PHONY: clean install

clean : 
	rm -f *.cmx *.cmi *.cmo *.a *.o vcli *~ *.annot

install : vcli
	mkdir -p $(DESTDIR)/opt/xensource/debug
	cp vcli $(DESTDIR)/opt/xensource/debug
	mkdir -p $(DESTDIR)/var/xapi/sm/
	cp attach_from_config_cli $(DESTDIR)/opt/xensource/libexec/

	cd $(DESTDIR)/var/xapi/sm/ &&	ln -s ../../../opt/xensource/libexec/attach_from_config_cli ./extSR
	cd $(DESTDIR)/var/xapi/sm/ &&	ln -s ../../../opt/xensource/libexec/attach_from_config_cli ./nfsSR
	cd $(DESTDIR)/var/xapi/sm/ &&	ln -s ../../../opt/xensource/libexec/attach_from_config_cli ./lvmoiscsiSR
	cd $(DESTDIR)/var/xapi/sm/ &&	ln -s ../../../opt/xensource/libexec/attach_from_config_cli ./lvmohbaSR
	cd $(DESTDIR)/var/xapi/sm/ &&	ln -s ../../../opt/xensource/libexec/attach_from_config_cli ./lvmSR


