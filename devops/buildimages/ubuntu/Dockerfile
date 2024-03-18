ARG BASE_IMAGE
FROM ${BASE_IMAGE}

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 51716619E084DAB9 && \
    add-apt-repository 'deb [trusted=yes] https://cloud.r-project.org/bin/linux/ubuntu xenial-cran35/' && \
    apt update && \
    apt upgrade -y ca-certificates && \
    apt -y install r-base
