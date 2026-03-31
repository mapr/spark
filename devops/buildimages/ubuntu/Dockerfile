ARG BASE_IMAGE
FROM ${BASE_IMAGE}

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends software-properties-common dirmngr && \
    apt-get install --reinstall python3-apt && \
    wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc && \
    /usr/bin/python3.6 /usr/bin/add-apt-repository -y "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran35/" && \
    apt-get install -y --no-install-recommends r-base

RUN unlink /opt/maven && \
    mkdir /opt/maven && \
    curl -s https://archive.apache.org/dist/maven/maven-3/3.9.11/binaries/apache-maven-3.9.11-bin.tar.gz | tar -xz --strip-components=1 -C /opt/maven
