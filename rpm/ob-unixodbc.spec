# (C) 2007-2010 TaoBao Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# obproxy.spec
#
# Version: $id$
#
Name: %NAME
Version: %VERSION
Release: %(echo %RELEASE)%{?dist}
License: LGPL & GPL
Group: applications/database
buildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
Autoreq: no
Prefix: /u01/oceanbase
Summary: ODBC Driver
BuildRequires: libtool 
BuildRequires: libtool-ltdl-devel



%define install_dir /u01/unix-odbc
%define work_root %(pwd)/../
%description
This is my odbc RPM package

%prep
# we have no source, so nothing here

%build
cd %{work_root}/unixODBC-2.3.7
sh run_make.sh

%install
echo "install now"
rm -rf $RPM_BUILD_ROOT%{install_dir}/
mkdir -p $RPM_BUILD_ROOT%{install_dir}/lib
mkdir -p $RPM_BUILD_ROOT%{install_dir}/include
mkdir -p $RPM_BUILD_ROOT%{install_dir}/bin
cd  %{work_root}/unixODBC-2.3.7/DriverManager/.libs/
tar czvf lib.tar.gz libodbc.*
cd  %{work_root}/unixODBC-2.3.7/odbcinst/.libs/
tar czvf lib2.tar.gz libodbcinst.*
cd  %{work_root}/unixODBC-2.3.7/bin/
cp isql iusql odbc_config dltest odbcinst slencheck $RPM_BUILD_ROOT%{install_dir}/bin/
cd  %{work_root}/unixODBC-2.3.7/rpm/
tar zxvf %{work_root}/unixODBC-2.3.7/DriverManager/.libs/lib.tar.gz -C $RPM_BUILD_ROOT%{install_dir}/lib/
tar zxvf %{work_root}/unixODBC-2.3.7/odbcinst/.libs/lib2.tar.gz -C $RPM_BUILD_ROOT%{install_dir}/lib/
cp %{work_root}/unixODBC-2.3.7/unixodbc_conf.h %{work_root}/unixODBC-2.3.7/include/
cp %{work_root}/unixODBC-2.3.7/config.h %{work_root}/unixODBC-2.3.7/include/
cd  %{work_root}/unixODBC-2.3.7/include/
tar czvf header.tar.gz *.h
tar zxvf ./header.tar.gz -C $RPM_BUILD_ROOT%{install_dir}/include/
%files
%{install_dir}/*

%post

%changelog
# let skip this for now
