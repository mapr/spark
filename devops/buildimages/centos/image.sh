#!/bin/sh

BASE_IMAGE=${DOCKER_REGISTRY}/centos8-java11-gcc8
BASE_TAG=latest

BASEDIR=$(dirname $0)
DOCKERFILE_CHECKSUM=$(find "$BASEDIR" | cpio -o 2>/dev/null | md5sum | head -c 7)

IMAGE_NAME_FOR_BUILD=${BASE_IMAGE}-custom-${REPOSITORY_NAME}:${BASE_TAG}-${DOCKERFILE_CHECKSUM}

ensure_image() {
    docker image pull -q ${IMAGE_NAME_FOR_BUILD}
    retVal=$?
    if [ "$retVal" -ne 0 ]; then
        docker build -q ${BASEDIR} -f ${BASEDIR}/Dockerfile -t ${IMAGE_NAME_FOR_BUILD} --build-arg BASE_IMAGE=${BASE_IMAGE}:${BASE_TAG}
        docker push -q ${IMAGE_NAME_FOR_BUILD}
    fi
}

ensure_image >&2

echo ${IMAGE_NAME_FOR_BUILD}
