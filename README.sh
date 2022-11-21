#!/bin/bash

if [ "$1" = "cleanup" ]
then
  docker-compose down -v --remove-orphans
  exit
fi

docker-compose up setup
docker-compose ps

docker-compose up -d cli
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
