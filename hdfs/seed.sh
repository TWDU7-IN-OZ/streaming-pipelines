#!/bin/bash

set -e

$hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /tw/rawData/stationDataSF/checkpoints \
&& $hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /tw/rawData/stationDataSF/data \
&& $hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /tw/rawData/stationDataFrance/checkpoints \
&& $hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /tw/rawData/stationDataFrance/data \
&& $hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /tw/rawData/stationDataNYCv2/checkpoints \
&& $hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /tw/rawData/stationDataNYCv2/data \
&& $hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /tw/stationMart/checkpoints_v2 \
&& $hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /tw/stationMart/data
