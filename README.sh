#!/bin/bash

if [ "$1" = "cleanup" ]
then
  docker-compose down -v
  mvn clean
  exit
fi

mvn package || exit 1
if [ "$1" = "build" ]; then exit; fi

trap 'kill $(jobs -p) 2>/dev/null' EXIT

docker-compose up -d

echo "Waiting for the Kafka-Cluster to become ready..."
docker-compose exec cli cub kafka-ready -b kafka:9092 1 60 > /dev/null 2>&1 || exit 1

echo "Producing messages"
mvn exec:java@producer

echo "Reading messages"
mvn exec:java@consumer &
sleep 7
kill $(jobs -p)
sleep 2

echo "Re-Reading messages"
mvn exec:java@consumer &
sleep 7
kill $(jobs -p)
sleep 2
