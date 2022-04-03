#!/bin/bash

if [ "$1" = "cleanup" ]
then
  docker-compose down -v
  exit
fi

trap 'kill $(jobs -p) 2>/dev/null' EXIT

docker-compose up -d

echo "Waiting for the Kafka-Cluster to become ready..."
docker-compose exec cli cub kafka-ready -b kafka:9092 1 60 > /dev/null 2>&1 || exit 1
