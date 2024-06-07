# Ruuster
> rabbitmq & kafka killer 🐰💥🔫 


# Setup

1. Clone the repo
1. Open in vscode
1. In vscode run command "reopen in devcontainer"
1. Build & run in the terminal

```rs
// to run the server
cargo run --bin server --release'
```

```bash
# to run docker for local tracing and metrics
docker run -d -p6831:6831/udp -p6832:6832/udp -p16686:16686 -p14268:14268 -p4317:4317 jaegertracing/all-in-one:latest

# then enter http://localhost:16686 to open Jaeger UI
```
---
# Ruuster workspace


| Crate        | Description                                                     | 
|--------------|:----------------------------------------------------------------|
| queues       | core ruuster functionalities (queues, acks)
| exchanges    | library definig exchangers behavior / logic of message trasport | 
| protos       | library with definitions of gRPC services (proto files)         |
| ruuster-qrpc | library containing implementation of gRPC server         |
| utils        | library with common utilities                                   |
| apps         | crate containing binaries built using our project               |


