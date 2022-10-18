#!/bin/bash

REDHAT=`cat /etc/redhat-release|cut -d " " -f 7|cut -d "." -f 1`

if [ $# -ne 4 ]
then
	TOP_DIR=`pwd`/../
	PACKAGE=mysql-odbc
	VERSION=`cat mysql-odbc-VER.txt`
	RELEASE="test.el${REDHAT}"
else
	TOP_DIR=$1
	PACKAGE=$2
	VERSION=$3
	RELEASE="$4.el${REDHAT}"
  export AONE_BUILD_NUMBER=${4}
fi
rpmbuild -ba odbc.spec
