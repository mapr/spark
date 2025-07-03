ARG BASE_IMAGE
FROM ${BASE_IMAGE}

RUN apt-get update -qq && \
    apt-get install --no-install-recommends software-properties-common dirmngr && \
    apt-get install --reinstall python3-apt && \
    wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc && \
    add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran36/" && \
        apt-get install --no-install-recommends r-base
