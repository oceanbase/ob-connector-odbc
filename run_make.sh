#!/bin/sh
function clean()
{
  rm -rf CMakeFiles CMakeCache.txt
}
function all()
{
  clean
  dm
  libobclient
  odbc
}
function dm()
{
    echo "building unixodbc"
    cd ./unixODBC-2.3.7
    sh run_make.sh
    cd ../
    echo "end building"
}
function libobclient()
{
    echo "building libobclient"
    cd ./libobclient 
    sh build.sh
    cd ../
    echo "end building"
}

function odbc()
{
    echo "building odbc"
    mkdir build
    cd build
    cmake ../ -DCMAKE_BUILD_TYPE=RELEASE  -DCONC_WITH_UNIT_TESTS=Off -DCMAKE_INSTALL_PREFIX=/u01/ob-connector-odbc -DWITH_SSL=OPENSSL -DDM_DIR=./unixODBC-2.3.7
    make -j `cat /proc/cpuinfo | grep processor| wc -l`
    echo "end building"
    cd ..
}

echo "
*******************************
The following is build option
*******************************
    1) build the whole project
    2) build libobclient
    3) build dirver manager
    4) build oceanbase odbc
******************************"
read -p "please enter your choice:" option
case $option in
1)
  all
;;
2)
  libobclient
;;
3)
  dm
;;
4)
  odbc
;;
*)
 echo "Invalid option number"
esac
