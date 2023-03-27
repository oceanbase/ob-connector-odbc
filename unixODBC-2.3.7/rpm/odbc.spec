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
Name:       unix-odbc
Version:    1.0.0
Release:    1
Summary:    unix-odbc driver
License:    FIXME

%define install_dir /u01/unix-odbc
%define work_root %(pwd)/../
%description
This is my odbc RPM package

%prep
# we have no source, so nothing here

%build
#cd %{work_root}
#sh run_make.sh

%install
echo "install now"
rm -rf $RPM_BUILD_ROOT%{install_dir}/
mkdir -p $RPM_BUILD_ROOT%{install_dir}/lib
cd  %{work_root}/DriverManager/.libs/
tar czvf lib.tar.gz libodbc.so.2 libodbc.so.2.0.0 libodbc.so
tar zxvf ./lib.tar.gz -C $RPM_BUILD_ROOT%{install_dir}/lib/
%files
%{install_dir}/lib/libodbc.so.2.0.0
%{install_dir}/lib/libodbc.so.2
%{install_dir}/lib/libodbc.so

%post

%changelog
# let skip this for now
