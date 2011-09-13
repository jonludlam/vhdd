VERSION = 0.1

DESTDIR ?= /
VERSION := $(shell hg parents --template "{rev}" 2>/dev/null || echo 0.0)

OCAMLABI := $(shell ocamlc -version)
OCAMLLIBDIR := $(shell ocamlc -where)
OCAMLDESTDIR ?= $(OCAMLLIBDIR)

OCAMLC = ocamlfind ocamlc -g
OCAMLOPT = ocamlfind ocamlopt -g
OCAMLPACKS = str,xapi-client,xmlm,xml-light2,stdext,uuid,camldm,lvm,rpc-light.syntax,rpc-light.json,netdev,threads,vhd,tapctl,http-svr
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
	ocamlfind ocamldep -package str,xapi-client,xmlm,xml-light2,stdext,uuid,camldm,lvm,rpc-light.syntax,rpc-light.json,netdev,threads,vhd -syntax camlp4o -I ../smapi  *.ml *.mli > deps

include deps
