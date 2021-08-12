#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "====Building Producer JARs===="
$DIR/../CitibikeApiProducer/gradlew -p $DIR/../CitibikeApiProducer clean bootJar

echo "====Building Consumer JARs===="
cd $DIR/../RawDataSaver && sbt package
cd $DIR/../StationConsumer && sbt package

echo "====Running docker-compose===="
$DIR/../docker-e2e/docker-compose.sh --project-directory $DIR/../docker-e2e -f $DIR/../docker-e2e/docker-compose.yml up --build -d

echo "====Wait for setup to complete===="
sleep 5

while [[ "`docker inspect -f {{.State.Running}} streamingdatapipeline_zookeeper-seed_1`" == "true" ]]; do
  echo "Waiting for zookeeper seed to complete..." 
  sleep 2
done

while [[ "`docker inspect -f {{.State.Running}} streamingdatapipeline_hadoop-seed_1`" == "true" ]]; do
  echo "Waiting for hadoop seed to complete..."
  sleep 2
done
