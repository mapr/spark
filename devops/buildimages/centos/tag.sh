#!/bin/sh

dockerfile_checksum=$(md5sum "devops/buildimages/centos/Dockerfile" | head -c 7)

echo "${DOCKER_REGISTRY}/centos8-java11-gcc8:spark332-${dockerfile_checksum}"
