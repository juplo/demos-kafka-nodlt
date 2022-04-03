#!/bin/bash

if [ "$1" = "cleanup" ]
then
  docker-compose down -v
  exit
fi

docker-compose up -d

echo "Waiting for the Kafka-Cluster to become ready..."
docker-compose exec cli cub kafka-ready -b kafka:9092 1 60 > /dev/null 2>&1 || exit 1

docker-compose ps

echo
echo "Hilfe-Ausgabe von kafkacat"
echo
docker-compose exec -T cli kafkacat -h
echo
echo "Nachrichten schreiben mit kafkacat"
echo
docker-compose exec -T cli kafkacat -P -b kafka:9092 -t test << EOF
Hallo Welt!
Nachricht #1
Nachricht #2
Nachricht #3
EOF
echo
echo "Nachrichten lesen mit kafkacat"
echo
docker-compose exec cli kafkacat -C -b kafka:9092 -t test -o beginning -e
