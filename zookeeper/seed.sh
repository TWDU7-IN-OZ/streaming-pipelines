#!/bin/sh
echo $zk_command
$zk_command rmr /tw
$zk_command create /tw ''

$zk_command create /tw/stationDataSF ''
$zk_command create /tw/stationDataSF/kafkaBrokers $kafka_server
$zk_command create /tw/stationDataSF/topic station_data_sf
$zk_command create /tw/stationDataSF/checkpointLocation hdfs://$hdfs_server/tw/rawData/stationDataSF/checkpoints
$zk_command create /tw/stationDataSF/dataLocation hdfs://$hdfs_server/tw/rawData/stationDataSF/data

$zk_command create /tw/stationDataFrance ''
$zk_command create /tw/stationDataFrance/kafkaBrokers $kafka_server
$zk_command create /tw/stationDataFrance/topic station_data_france
$zk_command create /tw/stationDataFrance/checkpointLocation hdfs://$hdfs_server/tw/rawData/stationDataFrance/checkpoints
$zk_command create /tw/stationDataFrance/dataLocation hdfs://$hdfs_server/tw/rawData/stationDataFrance/data

$zk_command create /tw/stationDataNYCv2 ''
$zk_command create /tw/stationDataNYCv2/kafkaBrokers $kafka_server
$zk_command create /tw/stationDataNYCv2/topic station_data_nyc_v2
$zk_command create /tw/stationDataNYCv2/checkpointLocation hdfs://$hdfs_server/tw/rawData/stationDataNYCv2/checkpoints
$zk_command create /tw/stationDataNYCv2/dataLocation hdfs://$hdfs_server/tw/rawData/stationDataNYCv2/data

$zk_command create /tw/output ''
$zk_command create /tw/output/checkpointLocation hdfs://$hdfs_server/tw/stationMart/checkpoints_v2
$zk_command create /tw/output/checkpointLocation1 hdfs://$hdfs_server/tw/stationMart/checkpoints1
$zk_command create /tw/output/dataLocation hdfs://$hdfs_server/tw/stationMart/data
