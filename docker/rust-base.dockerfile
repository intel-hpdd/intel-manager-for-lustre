FROM centos:7
WORKDIR /build
ARG toolchain=stable
RUN yum update -y \
  && yum install -y gcc openssl openssl-devel epel-release \
  && yum clean all \
  && yum clean all \
  && cd /root \
  && curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain $toolchain

ENV PATH $PATH:/root/.cargo/bin
ENV CARGO_HOME /root/.cargo
ENV RUSTUP_HOME /root/.rustup
COPY . .
RUN cargo build --release