ARG DOCKER_REGISTRY
FROM ${DOCKER_REGISTRY}/ubuntu16-java11-gcc7

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 51716619E084DAB9 && \
    add-apt-repository 'deb [trusted=yes] https://cloud.r-project.org/bin/linux/ubuntu xenial-cran35/' && \
    apt update && \
    apt -y install r-base
