Name:           bucket_engine
Version:        0.1
Release:        1
Summary:        Bucket engine for memcached
Group:          System Environment/Libraries
License:        DSAL
URL:            http://github.com/northscale/bucket_engine
Source0:        http://github.com/northscale/bucket_engine/downloads/%{name}-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-%(%{__id_u} -n)

%description
This memcached engine provides multi-tenancy and isolation between other
memcached engine instances.

%prep
%setup -q -n %{name}-%{version}

%build
%configure

make %{?_smp_mflags}

%check
make test

%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot}
find $RPM_BUILD_ROOT -type f -name '*.la' -exec rm -f {} ';'

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%dir %attr (0755, root, bin) %{_libdir}
%dir %attr (0755, root, bin) %{_datadir}
%attr (-, root, bin) %{_libdir}/bucket_engine.so*
%attr (-, root, bin) %{_datadir}/bucket_engine/*

%changelog
* Mon Feb  8 2010 Trond Norbye <trond.norbye@gmail.com> - 1.0-1
- Initial
