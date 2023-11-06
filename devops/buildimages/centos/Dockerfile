ARG DOCKER_REGISTRY
FROM ${DOCKER_REGISTRY}/centos8-java11-gcc8

RUN sed -i 's/^mirrorlist/#mirrorlist/' /etc/yum.repos.d/CentOS-* && \
    sed -i 's/^#baseurl/baseurl/' /etc/yum.repos.d/CentOS-* && \
    sed -i 's/mirror.centos.org/vault.centos.org/' /etc/yum.repos.d/CentOS-* && \
    yum install -y R
