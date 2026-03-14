#!/bin/bash

IMAGE=juplo/simple-consumer:1.0-SNAPSHOT

if [ "$1" = "cleanup" ]
then
  docker compose -f docker/docker-compose.yml down -t0 -v --remove-orphans
  mvn clean
  exit
fi

docker compose -f docker/docker-compose.yml up -d --remove-orphans kafka-1 kafka-2 kafka-3
docker compose -f docker/docker-compose.yml rm -svf consumer

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
docker compose -f docker/docker-compose.yml up -d consumer

sleep 5
docker compose -f docker/docker-compose.yml stop consumer

docker compose -f docker/docker-compose.yml start consumer
sleep 5

docker compose -f docker/docker-compose.yml stop producer consumer
docker compose -f docker/docker-compose.yml logs consumer
