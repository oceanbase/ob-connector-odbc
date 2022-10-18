#!/bin/sh
function all()
{
  libobclient
  dm
  odbc
  
}
function dm()
{
  echo "Building unixODBC rpm package"
  cd rpm
  sh ob-unixodbc-build.sh
  echo "End building"
  cd ../
}
function libobclient()
{
echo "Building libobclient rpm package"
cd ./libobclient/rpm 
chmod +x  libobclient-build.sh
sh libobclient-build.sh
cd ../../
echo "End building"
}

function odbc()
{
    echo "Building OceanBase odbc rpm package"
    sh rpm/ob-connector-odbc-build.sh
    echo "End building"
}
echo "
***********************************
The following is rpm build option
***********************************
    1) rpm build the whole project
    2) rpm build libobclient
    3) rpm build dirver manager
    4) rpm build oceanbase odbc
**********************************"
read -p "please enter your chioce:" option
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
