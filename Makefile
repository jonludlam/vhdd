
.PHONY: default vhdd clean install test

default : vhdd

vhdd : 
	make -C vhd

install : vhdd
	make -C vhd install
	make -C scripts install

clean : 
	make -C test clean
	make -C vhd clean

test :
	make -C test

