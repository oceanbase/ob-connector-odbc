#!/bin/bash
autoreconf -ivf
#autoreconf -f -i
automake
./configure --prefix=/u01/oceanbase
#CFLAGS="-g -DODBC_DEBUG=1" CPPFLAGS="-DODBC_DEBUG=1" ./configure --prefix=/usr/local
make
mkdir libdir
cp ./DriverManager/.libs/libodbc.so ./libdir
cp ./cur/.libs/libodbccr.so ./libdir
cp ./odbcinst/.libs/libodbcinst.so ./libdir
cp ./exe/odbc_config ./bin/ -rf


