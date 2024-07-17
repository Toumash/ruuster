## Ruuster Test Client

---
This application is advanced development tool to set up and run various scenarios using Ruuster project

---

### Scenario definition

To set up scenario you have to create a json file with information about desired system state

- queues
- exchanges
- bindings
- producers
- consumers

#### Example

```json
{
    "server_metadata": {
        "name": "SCENARIO_NAME",
        "server_addr": "SERVER_ADDR",
        "comment": "SCENARIO_DESC"
    },
    "queues": [
        {
            "name": "q0"
        }
    ],
    "exchanges": [
        {
            "name": "e0",
            "kind": 0,
            "bindings": [
                {
                    "queue_name": "q0",
                    "bind_metadata": null
                }
            ]
        }
    ],
    "producers": [
        {
            "name": "p0",
            "destination": "e0",
            "messages_produced": 5,
            "message_payload_bytes": 100,
            "post_message_delay_ms": 100
        }
    ],
    "consumers": [
        {
            "name": "c0",
            "source": "q0",
            "consuming_method": "stream",
            "ack_method": "auto",
            "workload_ms": {
                "min": 100,
                "max": 500
            }
        }
    ]
}
```

---

### Scenario validation

There is a tool for checking if json file with scenario configuration is valid

```
Usage: scenario_validator --config-file <CONFIG_FILE>

Options:
--config-file <CONFIG_FILE>  
```

---

### Scenario visualization

In scripts directory there is a python file for creating graph from json
using Graphviz library

```bash
python vizualize_scenario.py <CONFIG_FILE>
```

---

### Running scenario

You can execute defined scenario using run_scenario binary

```
Usage: run_scenario --server-addr <SERVER_ADDR> --config-file <CONFIG_FILE> --builder-bin <BUILDER_BIN> --consumer-bin <CONSUMER_BIN> --producer-bin <PRODUCER_BIN>

Options:
      --server-addr <SERVER_ADDR>    
      --config-file <CONFIG_FILE>    
      --builder-bin <BUILDER_BIN>    
      --consumer-bin <CONSUMER_BIN>  
      --producer-bin <PRODUCER_BIN>  
```

Each step of execution is separate program run as child process of run_scenario

#### Builder

This program is responsible for creating queues, exchanges and bindings.
It takes the same json file as run_scenario

```
Usage: scenario_builder --server-addr <SERVER_ADDR> --config-file <CONFIG_FILE>

Options:
      --server-addr <SERVER_ADDR>  
      --config-file <CONFIG_FILE>
```

#### Consumer

This program is responsible for consuming messages from one source/queue.
run_scenario will execute consumers based on json config file concurrently - each in its own process.
It will stop executing after 5s of idle (none messages consumed)
```
Usage: consumer [OPTIONS] --server-addr <SERVER_ADDR> --source <SOURCE> --consuming-method <CONSUMING_METHOD> --ack-method <ACK_METHOD>

Options:
      --server-addr <SERVER_ADDR>            
      --source <SOURCE>                      
      --consuming-method <CONSUMING_METHOD>  [possible values: single, stream]
      --ack-method <ACK_METHOD>              [possible values: auto, single, bulk]
      --min-delay-ms <MIN_DELAY_MS>          [default: 0]
      --max-delay-ms <MAX_DELAY_MS>          [default: 0]
```

#### Producer

This program will produce messages and send them to one exchange. Similar to a consumers, each producer runs in
independently. It will stop after sending amount of messages defined in config json

``` 
Usage: producer [OPTIONS] --server-addr <SERVER_ADDR> --destination <DESTINATION> --messages-produced <MESSAGES_PRODUCED> --message-payload-bytes <MESSAGE_PAYLOAD_BYTES> --delay-ms <DELAY_MS>

Options:
      --server-addr <SERVER_ADDR>                      
      --destination <DESTINATION>                      
      --messages-produced <MESSAGES_PRODUCED>          
      --message-payload-bytes <MESSAGE_PAYLOAD_BYTES>  
      --delay-ms <DELAY_MS>                            
      --metadata <METADATA> <- optional json with metadata
```