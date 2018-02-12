#!/bin/sh

dt=`date -d "-31 days" +%Y%m%d`
echo $dt

dir=$1
cd $dir
for k in $(ls $dir)
do
    log_d=${k:4:8}
    if [ $log_d -eq $dt ]
    then
	echo $k
	rm -f $k
    fi
done

dt=`date -d "-7 days" +%Y%m%d`
for k in $(ls $dir)
do
    log_d=${k:4:8}
    if [ $log_d -eq $dt ]
    then
	echo $k
	gzip $k
    fi
done
