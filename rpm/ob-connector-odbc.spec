Name: %NAME
Version: %VERSION
Release: %(echo %RELEASE)%{?dist}
License: LGPL
Group: applications/database
buildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
Packager: guangshu.wgs@antfin.com
Autoreq: no
Prefix: /u01/obclient
Summary: Oracle 5.6 and some patches from Oceanbase
BuildRequires: libtool 
BuildRequires: libtool-ltdl-devel 
Requires: libobclient >= 2.1.2

%description
The MySQL(TM) software delivers a very fast, multi-threaded, multi-user,
and robust SQL (Structured Query Language) database server. MySQL Server
is intended for mission-critical, heavy-load production systems as well
as for embedding into mass-deployed software.

%define MYSQL_USER root
%define MYSQL_GROUP root
%define __os_install_post %{nil}
%define base_dir /u01/mysql
%define file_dir /app/mariadb


%prep
cd $OLDPWD/../

#%setup -q
%build
cd $OLDPWD/../
echo "Building libobclient rpm package"
cd ./libobclient/rpm 
chmod +x  libobclient-build.sh
sh libobclient-build.sh
cd ../../
echo "End building"
./build.sh --prefix %{prefix} --version %{version}

%install
cd $OLDPWD/../build
make DESTDIR=$RPM_BUILD_ROOT install
find $RPM_BUILD_ROOT -name '.git' -type d -print0|xargs -0 rm -rf
mkdir -p $RPM_BUILD_ROOT%{prefix}

%clean
rm -rf $RPM_BUILD_ROOT

%files
#%defattr(-, %{MYSQL_USER}, %{MYSQL_GROUP})
#%attr(755, %{MYSQL_USER}, %{MYSQL_GROUP}) %{prefix}/*
#%dir %attr(755,  %{MYSQL_USER}, %{MYSQL_GROUP}) %{prefix}
#%attr(755, %{MYSQL_USER}, %{MYSQL_GROUP}) 
#%dir %attr(755,  %{MYSQL_USER}, %{MYSQL_GROUP})
/u01/ob-connector-odbc/*
%pre

%post

%preun

%changelog

