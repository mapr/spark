ARG BASE_IMAGE
FROM ${BASE_IMAGE}

RUN sed -i 's/^mirrorlist/#mirrorlist/' /etc/yum.repos.d/CentOS-* && \
    sed -i 's/^#baseurl/baseurl/' /etc/yum.repos.d/CentOS-* && \
    sed -i 's/mirror.centos.org/vault.centos.org/' /etc/yum.repos.d/CentOS-* && \
    yum install -y R
