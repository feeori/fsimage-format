#!/bin/bash

echo "load csv to hdfs.."
hadoop fs -mkdir /tmp/fsimage
hadoop fs -put ./output.csv /tmp/fsimage
hadoop fs -chmod 777 /tmp/fsimage

echo "load csv to impala.."
hive -f ./hdfs.sql

echo "remove csv.."
#hadoop fs -rm /tmp/fsimage/output.csv
