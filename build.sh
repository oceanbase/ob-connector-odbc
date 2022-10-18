#!/bin/sh
function clean()
{
  rm -rf CMakeFiles CMakeCache.txt
}
clean
echo "End building"
mkdir build
cd build
cmake ../ -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCONC_WITH_UNIT_TESTS=Off -DCMAKE_INSTALL_PREFIX=/u01/ob-connector-odbc  -DWITH_SSL=OPENSSL -DDM_DIR=./unixODBC-2.3.7
make -j `cat /proc/cpuinfo | grep processor| wc -l`
