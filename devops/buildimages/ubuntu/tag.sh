#!/bin/sh

dockerfile_checksum=$(md5sum "devops/buildimages/ubuntu/Dockerfile" | head -c 7)

echo "${DOCKER_REGISTRY}/ubuntu16-java11-gcc7:spark332-${dockerfile_checksum}"
