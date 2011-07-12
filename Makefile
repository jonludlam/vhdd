
.PHONY: default vhdd clean install

default : vhdd

vhdd : 
	make -C smapi
	make -C vhd
	make -C vcli

install : vhdd
	make -C vhd install
	make -C vcli install
	make -C scripts install

clean : 
	make -C smapi clean
	make -C vhd clean
	make -C vcli clean
