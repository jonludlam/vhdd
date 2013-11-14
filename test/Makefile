VERSION = 0.1

DESTDIR ?= /
VERSION := $(shell hg parents --template "{rev}" 2>/dev/null || echo 0.0)

OCAMLABI := $(shell ocamlc -version)
OCAMLLIBDIR := $(shell ocamlc -where)
OCAMLDESTDIR ?= $(OCAMLLIBDIR)

OCAMLC = ocamlfind ocamlc -g
OCAMLOPT = ocamlfind ocamlopt 
OCAMLPACKS = xcp,xcp-inventory,str,uuidm,camldm,lvm,rpclib.syntax,rpclib.json,netdev,threads,vhdlib,tapctl,http-svr,xcp.storage,cstruct,cstruct.syntax,zip
COMPFLAG = -thread -package $(OCAMLPACKS) -syntax camlp4o -I ../vhd -dtypes -g
LINKFLAG = -linkpkg 
SOURCES = \
	context \
	ocamltest \
	circ \
	olvm \
	int_types \
	extentlist \
	int_rpc \
	nmutex \
	lockgen \
	host \
	fsutils \
	lvmabs_types \
	global \
	vhdutil \
	vhdrpc \
	vhd_records \
	vhd_types \
	int_client \
	tapdisk_listen \
	tapdisk \
	tracelog \
	lvmabs \
	scan \
	errors \
	drivers \
	rwlock \
	debug_utils \
	nfs \
	iscsilib \
	transport \
	attachments \
	dot \
	html \
	slave_sr_attachments \
	locking \
	int_client_utils \
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

.PHONY: runtest

runtest : ftests
	./ftests -path ../vhd/vhdd

OBJS := $(foreach obj, $(SOURCES),../vhd/$(obj).cmx)

get_hidden_lvs_test: $(OBJS) get_hidden_lvs_test.cmx
	$(OCAMLOPT) $(COMPFLAG) $(LINKFLAG) -o $@ $(OBJS) get_hidden_lvs_test.cmx

test : $(OBJS) test.cmx util.cmx simpletest.cmx coalescetest.cmx paralleltest.cmx lockingtest.cmx 
	$(OCAMLOPT) $(COMPFLAG) $(LINKFLAG) -o $@ $(OBJS) util.cmx simpletest.cmx coalescetest.cmx paralleltest.cmx lockingtest.cmx test.cmx

ftests: $(OBJS) lockingtest.cmx ftests.cmx 
	$(OCAMLOPT) $(COMPFLAG) $(LINKFLAG) -o $@ $(OBJS) lockingtest.cmx ftests.cmx

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
	ocamlfind ocamldep -package str,xen-api-client,xmlm,xml-light2,stdext,uuid,camldm,lvm,rpclib.syntax,rpclib.json,netdev,threads,vhdlib,tapctl -syntax camlp4o -I ../vhd  *.ml *.mli > deps

include deps

