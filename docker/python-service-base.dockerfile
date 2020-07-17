FROM centos:7 as builder
WORKDIR /build
COPY . .
RUN yum update -y
RUN yum install -y rpmdevtools make git
RUN make -f .copr/Makefile base.repo

FROM centos:7

ENV HTTPS_FRONTEND_PORT 7443
ENV DB_HOST postgres
ENV DB_PORT 5432
ENV AMQP_BROKER_HOST rabbit
ENV SERVER_FQDN nginx
ENV LOG_PATH .

WORKDIR /usr/share/chroma-manager/
COPY . .
COPY --from=builder /build/base.repo .
RUN yum --disablerepo=epel -y update ca-certificates \
  && yum install -y epel-release \
  && yum clean all \
  && yum check-update epel-release \
  && yum install -y epel-release \
  && yum clean all \
  && yum update -y \
  && yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm \
  && yum install -y python python-pip python-devel postgresql96 openssl gcc-c++ \
  && pip install -r requirements.txt \
  && yum autoremove -y gcc-c++ python-pip python-devel \
  && rm -rf /root/.cache/pip \
  && yum install -y python-setuptools \
  && yum clean all

COPY docker/wait-for-dependencies-postgres.sh /usr/local/bin/
ENTRYPOINT [ "wait-for-dependencies-postgres.sh" ]
