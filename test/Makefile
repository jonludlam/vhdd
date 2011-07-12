VERSION = 0.1

DESTDIR ?= /
VERSION := $(shell hg parents --template "{rev}" 2>/dev/null || echo 0.0)

OCAMLABI := $(shell ocamlc -version)
OCAMLLIBDIR := $(shell ocamlc -where)
OCAMLDESTDIR ?= $(OCAMLLIBDIR)

OCAMLC = ocamlfind ocamlc -g
OCAMLOPT = ocamlfind ocamlopt 
OCAMLPACKS = str,xapi-client,xmlm,xml-light2,stdext,uuid,camldm,lvm,rpc-light.syntax,rpc-light.json,netdev,threads,vhd,tapctl
COMPFLAG = -thread -package $(OCAMLPACKS) -syntax camlp4o -I ../smapi -I ../vhd -dtypes -g
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
	debug_utils \
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
	int_server \
	fileserver 

OBJS := $(foreach obj, $(SOURCES),../vhd/$(obj).cmx)

get_hidden_lvs_test: $(OBJS) get_hidden_lvs_test.cmx
	$(OCAMLOPT) $(COMPFLAG) $(LINKFLAG) -o $@ $(OBJS) get_hidden_lvs_test.cmx

test : $(OBJS) test.cmx util.cmx simpletest.cmx coalescetest.cmx paralleltest.cmx lockingtest.cmx 
	$(OCAMLOPT) $(COMPFLAG) $(LINKFLAG) -o $@ $(OBJS) util.cmx simpletest.cmx coalescetest.cmx paralleltest.cmx lockingtest.cmx test.cmx

ftests: $(OBJS) lockingtest.cmx ftests.cmx 
	$(OCAMLOPT) $(COMPFLAG) $(LINKFLAG) -o $@ $(OBJS) lockingtest.cmx ftests.cmx

#install : vhdd
#	mkdir -p $(DESTDIR)/opt/xensource/libexec
#	cp vhdd $(DESTDIR)/opt/xensource/libexec

.SUFFIXES: .ml .mli .cmo .cmi .cmx

.ml.cmo:
	$(OCAMLC) $(COMPFLAG) -c $<

.mli.cmi:
	$(OCAMLOPT) $(COMPFLAG) -c $<

.ml.cmx:
	$(OCAMLOPT) $(COMPFLAG) -c $<

.PHONY: clean

clean:
	rm -f *.cmi *.cmx *.cmo *.a *.cma *.cmxa *.run *.opt *.annot *.o
	rm -f get_hidden_lvs_test

deps : 
	ocamlfind ocamldep -package str,xapi-client,xmlm,xml-light2,stdext,uuid,camldm,lvm,rpc-light.syntax,rpc-light.json,netdev,threads,vhd -syntax camlp4o -I ../smapi -I ../vhd  *.ml *.mli > deps

include deps

