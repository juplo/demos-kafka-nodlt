#!/bin/bash

IMAGE=juplo/nodlt:1.0-SNAPSHOT

if [ "$1" = "cleanup" ]
then
  docker compose -f docker/docker-compose.yml down -t0 -v --remove-orphans
  mvn clean
  exit
fi

docker compose -f docker/docker-compose.yml up -d --remove-orphans kafka-1 kafka-2 kafka-3
docker compose -f docker/docker-compose.yml rm -svf nodlt

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

docker compose -f docker/docker-compose.yml up -d producer consumer nodlt

while ! [[ $(http 0:8881/actuator/health 2> /dev/null) =~ "UP" ]]; do echo "Waiting for nodlt..."; sleep 1; done

http -v :8881/0/0
http -v :8881/1/3

docker compose -f docker/docker-compose.yml stop producer consumer
