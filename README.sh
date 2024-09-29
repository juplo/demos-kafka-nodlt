#!/bin/bash

IMAGE=juplo/simple-producer:1.0-SNAPSHOT

if [ "$1" = "cleanup" ]
then
  docker compose -f docker/docker-compose.yml down -t0 -v --remove-orphans
  mvn clean
  exit
fi

docker compose -f docker/docker-compose.yml up -d --remove-orphans kafka-1 kafka-2 kafka-3
docker compose -f docker/docker-compose.yml rm -svf producer

if [[
  $(docker image ls -q $IMAGE) == "" ||
  "$1" = "build"
]]
then
  mvn clean install || exit
else
  echo "Using image existing images:"
  docker image ls $IMAGE
fi

docker compose -f docker/docker-compose.yml up --remove-orphans setup || exit 1


docker compose -f docker/docker-compose.yml up -d producer
sleep 5

docker compose -f docker/docker-compose.yml exec cli kafkacat -b kafka:9092 -t test -c 20 -f'topic=%t\tpartition=%p\toffset=%o\tkey=%k\tvalue=%s\n'

docker compose -f docker/docker-compose.yml stop producer
docker compose -f docker/docker-compose.yml exec cli kafkacat -b kafka:9092 -t test -e -f'topic=%t\tpartition=%p\toffset=%o\tkey=%k\tvalue=%s\n'
docker compose -f docker/docker-compose.yml logs producer
