VERSION = 0.1

DESTDIR ?= /
VERSION := $(shell hg parents --template "{rev}" 2>/dev/null || echo 0.0)

OCAMLABI := $(shell ocamlc -version)
OCAMLLIBDIR := $(shell ocamlc -where)
OCAMLDESTDIR ?= $(OCAMLLIBDIR)
LINKFLAG = -linkpkg
OCAMLC = ocamlfind ocamlc -g
OCAMLOPT = ocamlfind ocamlopt -g
OCAMLPACKS_COMMON = xcp,xcp-inventory,str,uuidm,camldm,lvm,rpclib.json,netdev,threads,vhdlib,tapctl,http-svr,xcp.storage,cstruct,cstruct.syntax,zip
OCAMLPACKS = $(OCAMLPACKS_COMMON),rpclib.syntax
OCAMLPACKS_IDL = $(OCAMLPACKS_COMMON),rpclib.idl
COMPFLAG = -thread -package $(OCAMLPACKS) -syntax camlp4o -dtypes
COMPFLAG_IDL = -thread -package $(OCAMLPACKS_IDL) -syntax camlp4o -dtypes
SOURCES = \
	context \
	ocamltest \
	circ \
	olvm \
	int_types \
	extentlist \
	junk \
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
	int_rpc \
	int_client \
	tapdisk_listen \
	tapdisk \
	tracelog \
	lvmabs \
	scan \
	errors \
	drivers \
	rwlock \
	vhdd_helper \
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
	debug_utils \
	vhdsm \
	int_server \
	fileserver \
	vhdd 

OBJS = $(foreach obj, $(SOURCES),$(obj).cmx)
MLS = $(foreach obj, $(SOURCES),$(obj).ml)

vhdd : $(OBJS)
	$(OCAMLOPT) $(COMPFLAG) $(LINKFLAG) -o $@ $(OBJS)

install : vhdd
	mkdir -p $(DESTDIR)/opt/xensource/libexec
	cp vhdd $(DESTDIR)/opt/xensource/libexec

.SUFFIXES: .ml .mli .cmo .cmi .cmx

int_rpc.cmx: int_rpc.ml
	$(OCAMLOPT) $(COMPFLAG_IDL) -c $<

int_rpc.cmi:int_rpc.ml
	$(OCAMLOPT) $(COMPFLAG_IDL) -c $<

.ml.cmo:
	$(OCAMLC) $(COMPFLAG) -c $<

.mli.cmi:
	$(OCAMLOPT) $(COMPFLAG) -c $<

.ml.cmx:
	$(OCAMLOPT) $(COMPFLAG) -c $<


.PHONY: clean 

clean:
	rm -f *.cmi *.cmx *.cmo *.a *.cma *.cmxa *.run *.opt *.annot *.o

deps:
	ocamlfind ocamldep -package str,xen-api-client,xmlm,xml-light2,stdext,uuid,camldm,lvm,rpclib.syntax,rpclib.json,netdev,threads,vhdlib,cstruct,cstruct.syntax -syntax camlp4o  *.ml *.mli > deps

include deps
