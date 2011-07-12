# -*- rpm-spec -*-

%define XEN_RELEASE %(test -z "${XEN_RELEASE}" && echo unknown || echo $XEN_RELEASE)

Summary: vhdd - daemon for managing VHD based SRs for XCP
Name:    vhdd
Version: 0
Release: %{XEN_RELEASE}
Group:   System/Hypervisor
License: LGPL+linking exception
URL:  http://www.xen.org
Source0: vhdd-%{version}.tar.bz2
BuildRoot: %{_tmppath}/%{name}-%{version}-root

%description
A daemon for managing VHD based SRs for XCP

%prep 
%setup -q
%build
%{__make} vhdd 

%install
rm -rf %{buildroot}

DESTDIR=$RPM_BUILD_ROOT %{__make} install

%clean
rm -rf $RPM_BUILD_ROOT

%post 
[ ! -x /sbin/chkconfig ] || chkconfig vhdd on
[ ! -x /sbin/chkconfig ] || chkconfig vhdd-clean on



%files 
%defattr(-,root,root,-)
/etc/rc.d/init.d/vhdd
/etc/rc.d/init.d/vhdd-clean
/opt/xensource/libexec/vhdd
/opt/xensource/debug/vcli
/opt/xensource/libexec/rolling-upgrade-finished.sh
/etc/xensource/bugtool/xenserver-logs/vhdd.xml
/opt/xensource/libexec/attach_from_config_cli
/var/xapi/sm/lvmSR
/var/xapi/sm/nfsSR
/var/xapi/sm/extSR
/var/xapi/sm/lvmoiscsiSR
/var/xapi/sm/lvmohbaSR
%changelog
