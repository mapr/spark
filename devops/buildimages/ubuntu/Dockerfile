ARG BASE_IMAGE
FROM ${BASE_IMAGE}

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends software-properties-common dirmngr && \
    apt-get install --reinstall python3-apt && \
    wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc && \
    /usr/bin/python3.6 /usr/bin/add-apt-repository -y "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran35/" && \
    apt-get install -y --no-install-recommends r-base
