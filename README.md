# Ruuster
> rabbitmq & kafka killer 🐰💥🔫 


# Setup

1. Clone the repo
1. Open in vscode
1. In vscode run command "reopen in devcontainer"
1. Build & run in the terminal

```rs
# to run the server
cargo watch -x 'run --bin server'

# to run the client
cargo run --bin client


# to run with a specified log level
RUST_LOG=debug cargo run --bin client
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


