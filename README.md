# Ruuster
> rabbitmq & kafka killer 🐰💥🔫 


# Setup

To run you can use GitPod (super fast), devcontainers (local docker + vscode) or just linux box

1. Clone the repo
1. Open in vscode
1. Run `docker compose up -d` to start logs/traces collector UI: http://localhost:16686
1. Build & run in the terminal

```bash
# to run the server
cargo watch -x 'run --bin server'  --ignore protos/
# to have watch on tests
cargo watch -x 'test'  --ignore protos/
```

# Docker demo

```bash
# pull the published image
docker pull ghcr.io/toumash/ruuster:latest
# run the server container
docker run --rm -p 50051:50051 -e RUUSTER_SERVER_ADDR=0.0.0.0:50051 ghcr.io/toumash/ruuster:latest
# run a demo client against the container
cargo run --bin docker_demo -- --server-addr "http://127.0.0.1:50051"
```
---
# Ruuster workspace


| Crate        | Description                                                       | 
|--------------|:------------------------------------------------------------------|
| queues       | core ruuster functionalities (queues, acks)                       |
| internals    | declarations of internal types used by ruuster                    |
| exchanges    | library defining exchangers behavior / logic of message trasport  | 
| protos       | library with definitions of gRPC services (proto files)           |
| ruuster-qrpc | library containing implementation of gRPC server                  |
| utils        | library with common utilities                                     |
| apps         | crate containing binaries built using our project                 |


