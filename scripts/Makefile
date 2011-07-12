.phony: install

install :
	mkdir -p $(DESTDIR)/etc/rc.d/init.d
	mkdir -p $(DESTDIR)/opt/xensource/libexec
	cp init.d-vhdd $(DESTDIR)/etc/rc.d/init.d/vhdd
	cp init.d-vhdd-clean $(DESTDIR)/etc/rc.d/init.d/vhdd-clean
	cp rolling-upgrade-finished.sh $(DESTDIR)/opt/xensource/libexec
	chmod 755 $(DESTDIR)/opt/xensource/libexec/rolling-upgrade-finished.sh
	chmod 755 $(DESTDIR)/etc/rc.d/init.d/*
	mkdir -p $(DESTDIR)/etc/xensource/bugtool/xenserver-logs
	cp xenserver-logs-vhdd.xml $(DESTDIR)/etc/xensource/bugtool/xenserver-logs/vhdd.xml
