#!/bin/sh
echo "Cleaning libobclient"
cd ./libobclient 
make clean
cd ../
echo "End cleaning"
echo "Cleaning unixODBC"
cd ./unixODBC-2.3.7
sh run_clean.sh
cd ../
echo "End cleaning"
echo "Cleaning OceanBase odbc"
rm -rf build/*
echo "End cleaning"
