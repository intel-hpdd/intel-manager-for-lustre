FROM rust-iml-base as builder
FROM imlteam/rust-service-base:6.2.0

COPY --from=builder /build/target/release/iml-corosync /usr/local/bin
COPY docker/wait-for-dependencies-postgres.sh /usr/local/bin

ENTRYPOINT [ "wait-for-dependencies-postgres.sh" ]
CMD ["iml-corosync"]
