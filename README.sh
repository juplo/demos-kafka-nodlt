#!/bin/bash

if [ "$1" = "cleanup" ]
then
  docker-compose -f docker/docker-compose.yml down -t0 -v --remove-orphans
  exit
fi

docker-compose -f docker/docker-compose.yml up --remove-orphans setup || exit 1
docker-compose -f docker/docker-compose.yml ps

docker-compose -f docker/docker-compose.yml up -t0 -d cli
sleep 1
docker-compose -f docker/docker-compose.yml logs setup

echo
echo "Hilfe-Ausgabe von kafkacat"
echo
docker-compose -f docker/docker-compose.yml exec -T cli kafkacat -h
echo
echo "Nachrichten schreiben mit kafkacat"
echo
docker-compose -f docker/docker-compose.yml exec -T cli kafkacat -P -b kafka:9092 -t test << EOF
Hallo Welt!
Nachricht #1
Nachricht #2
Nachricht #3
EOF
echo
echo "Nachrichten lesen mit kafkacat"
echo
docker-compose -f docker/docker-compose.yml exec cli kafkacat -C -b kafka:9092 -t test -o beginning -e
