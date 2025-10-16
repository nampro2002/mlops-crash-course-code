#!/bin/bash

cmd=$1

# constants
DOCKER_USER="$DOCKER_USER"
PROJECT="mlops-monitoring-service"
IMAGE_TAG=$(git describe --always)

if [[ -z "$DOCKER_USER" ]]; then
    echo "Missing \$DOCKER_USER env var"
    exit 1
fi

usage() {
    echo "deploy.sh <command>"
    echo "Available commands:"
    echo " build                build image"
    echo " push                 push image"
    echo " build_push           build and push image"
    echo " compose_up           up docker compose"
    echo " compose_down         down docker compose"
}

if [[ -z "$cmd" ]]; then
    echo "Missing command"
    usage
    exit 1
fi

build() {
    docker build --tag $DOCKER_USER/$PROJECT:$IMAGE_TAG -f deployment/Dockerfile .
    docker tag $DOCKER_USER/$PROJECT:$IMAGE_TAG $DOCKER_USER/$PROJECT:latest
}

push() {
    docker push $DOCKER_USER/$PROJECT:$IMAGE_TAG
    docker push $DOCKER_USER/$PROJECT:latest
}

compose_up() {
    docker-compose --env-file ./deployment/.env -f ./deployment/docker-compose.yml up -d
}

compose_down() {
    docker-compose --env-file ./deployment/.env -f ./deployment/docker-compose.yml down
}

shift

case $cmd in
build)
    build "$@"
    ;;
push)
    push "$@"
    ;;
build_push)
    build "$@"
    push "$@"
    ;;
compose_up)
    compose_up "$@"
    ;;
compose_down)
    compose_down "$@"
    ;;
*)
    echo -n "Unknown command: $cmd"
    usage
    exit 1
    ;;
esac
