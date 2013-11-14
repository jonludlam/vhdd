VERSION = 0.1

DESTDIR ?= /
VERSION := $(shell hg parents --template "{rev}" 2>/dev/null || echo 0.0)

OCAMLABI := $(shell ocamlc -version)
OCAMLLIBDIR := $(shell ocamlc -where)
OCAMLDESTDIR ?= $(OCAMLLIBDIR)

LIBOBJS = smapi_types smapi process_xmlrpc smapi_client
INTF = $(foreach obj, $(LIBOBJS),$(obj).cmi)
CMXS = $(foreach obj, $(LIBOBJS),$(obj).cmx)
OCAMLC = ocamlfind ocamlc -g
OCAMLOPT = ocamlfind ocamlopt -g
COMPFLAG = -package xcp,rpclib.syntax,rpclib.json,xen-api-client,http-svr -syntax camlp4o -dtypes -thread

default : smapi.cmxa

smapi.cmxa: $(CMXS)
	$(OCAMLOPT) -a -g -o $@ $(CMXS)

.SUFFIXES: .ml .mli .cmo .cmi .cmx

.ml.cmo:
	$(OCAMLC) $(COMPFLAG) -c $<

.mli.cmi:
	$(OCAMLOPT) $(COMPFLAG) -c $<

.ml.cmx:
	$(OCAMLOPT) $(COMPFLAG) -c $<

.PHONY: clean
clean : 
	rm -f *.cmo *.cmi *.cmx *.o *~ *.annot smapi.cmxa smapi.a


