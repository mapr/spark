ARG BASE_IMAGE
FROM ${BASE_IMAGE}

RUN sed -i 's/^mirrorlist/#mirrorlist/' /etc/yum.repos.d/CentOS-* && \
    sed -i 's/^#baseurl/baseurl/' /etc/yum.repos.d/CentOS-* && \
    sed -i 's/mirror.centos.org/vault.centos.org/' /etc/yum.repos.d/CentOS-* && \
    yum install -y R

RUN unlink /opt/maven && \
    mkdir /opt/maven && \
    curl -s https://archive.apache.org/dist/maven/maven-3/3.9.11/binaries/apache-maven-3.9.11-bin.tar.gz | tar -xz --strip-components=1 -C /opt/maven
