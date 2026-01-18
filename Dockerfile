FROM rust:1.92-alpine AS builder

RUN apk add --no-cache musl-dev openssl-dev protobuf-dev pkgconfig

WORKDIR /app
COPY . .

RUN cargo build -p apps --bin server --release

FROM alpine:3.19

RUN apk add --no-cache ca-certificates libgcc libstdc++ openssl

WORKDIR /app
RUN mkdir -p /app/protos/defs

COPY --from=builder /app/target/release/server /app/server
COPY --from=builder /app/protos/defs/ruuster_descriptor.bin /app/protos/defs/ruuster_descriptor.bin

ENV RUUSTER_SERVER_ADDR=0.0.0.0:50051
EXPOSE 50051

CMD ["/app/server"]
