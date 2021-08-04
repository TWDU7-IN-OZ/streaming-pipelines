FROM ryandjf/docker-spark
ADD target/scala-2.11/tw-station-validator_2.11-0.0.1.jar /app.jar
ENTRYPOINT spark-submit --class com.tw.apps.StationValidatorApp --name StationValidatorApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0  /app.jar "zookeeper:2181" "/tw/stationMart/data/test.csv"
