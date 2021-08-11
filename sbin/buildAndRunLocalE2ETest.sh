#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "====Building Producer JARs===="
$DIR/../CitibikeApiProducer/gradlew -p $DIR/../CitibikeApiProducer clean bootJar
echo "====Building Consumer JARs===="
cd $DIR/../RawDataSaver && sbt package
cd $DIR/../StationConsumer && sbt package
cd $DIR/../StationTransformerNYC && sbt package
echo "====Running docker-compose===="
$DIR/../docker-e2e/docker-compose.sh --project-directory $DIR/../docker-e2e -f $DIR/../docker-e2e/docker-compose.yml up --build -d

