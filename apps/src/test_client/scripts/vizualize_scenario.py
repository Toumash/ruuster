from graphviz import Digraph
import random
import json
import sys

def load_config():
    with open(sys.argv[1]) as f:
        config = json.load(f)
        return config
    
data = load_config()

def generate_random_color():
    primary_component = random.choice([0, 255])
    
    components = [0, 0, 0]
    dominant_index = random.randint(0, 2)
    
    components[dominant_index] = primary_component
    
    if primary_component == 255:
        secondary_value = random.randint(0, 127)
    else:
        secondary_value = random.randint(128, 255)
    
    non_dominant_indices = {0, 1, 2} - {dominant_index}
    for index in non_dominant_indices:
        components[index] = secondary_value

    r, g, b = components
    return f'#{r:02x}{g:02x}{b:02x}'

def get_exchange_kind_name(kind):
    if kind == 0:
        return "Fanout"
    elif kind == 1:
        return "Direct"

def get_metadata_label(metadata_json):
    return f"""<<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0">
    <TR><TD>metadata</TD><TD></TD></TR>
    <TR><TD>name:</TD><TD>{metadata_json["name"]}</TD></TR>
    <TR><TD>server_addr:</TD><TD>{metadata_json["server_addr"]}</TD></TR>
    <TR><TD>comment:</TD><TD>{metadata_json["comment"]}</TD></TR>
    <TR><TD>scenario path:</TD><TD>{sys.argv[1]}</TD></TR>
    </TABLE>>"""

def get_producer_label(producer_json):
    return f"""<<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0">
    <TR><TD>producer</TD><TD></TD></TR>
    <TR><TD>name:</TD><TD>{producer_json["name"]}</TD></TR>
    <TR><TD>destination:</TD><TD>{producer_json["destination"]}</TD></TR>
    <TR><TD>messages_produced:</TD><TD>{producer_json["messages_produced"]}</TD></TR>
    <TR><TD>messages_payload_bytes:</TD><TD>{producer_json["message_payload_bytes"]}</TD></TR>
    </TABLE>>"""

def get_exchange_label(exchange_json):
    return f"""<<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0">
    <TR><TD>exchange</TD><TD></TD></TR>
    <TR><TD>name:</TD><TD>{exchange_json["name"]}</TD></TR>
    <TR><TD>kind:</TD><TD>{get_exchange_kind_name(exchange_json["kind"])}</TD></TR>
    <TR><TD>num of bindings:</TD><TD>{len(exchange_json["bindings"])}</TD></TR>
    </TABLE>>"""

def get_queue_label(queue_json):
    return f"""<<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0">
    <TR><TD>queue</TD><TD></TD></TR>
    <TR><TD>name:</TD><TD>{queue_json["name"]}</TD></TR>
    </TABLE>>"""

def get_consumer_label(consumer_json):
    return f"""<<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0">
    <TR><TD>consumer</TD><TD></TD></TR>
    <TR><TD>name:</TD><TD>{consumer_json["name"]}</TD></TR>
    <TR><TD>consuming_method:</TD><TD>{consumer_json["consuming_method"]}</TD></TR>
    <TR><TD>ack_method:</TD><TD>{consumer_json["ack_method"]}</TD></TR>
    <TR><TD>workload:</TD><TD>[{consumer_json["workload_ms"]["min"]}, {consumer_json["workload_ms"]["max"]}] [ms]</TD></TR>
    </TABLE>>"""

# Create a new directed graph
dot = Digraph(comment='scenario', format='png')

dot.attr(ranksep='2.0')  # Apply global rank separation for all ranks
dot.attr(nodesep='0.5') 

dot.node("metadata",label=get_metadata_label(data["server_metadata"]), shape="box")

# Add nodes and edges for producers and exchanges
for producer in data['producers']:
    dot.node(producer['name'], label=get_producer_label(producer), shape='box')
    dot.edge(producer['name'], producer['destination'])

with dot.subgraph() as sub:
    sub.attr(rank='same')
    for exchange in data['exchanges']:
        color = generate_random_color()
        sub.node(exchange['name'], label=get_exchange_label(exchange), shape='box', color=color)
        # Add edges from exchanges to queues
        for bind in exchange['bindings']:
            if bind["bind_metadata"] is not None:
                dot.edge(exchange['name'], bind["queue_name"], color=color, label=bind["bind_metadata"]["routing_key"])
            else:
                dot.edge(exchange['name'], bind["queue_name"], color=color)

with dot.subgraph() as sub:
    sub.attr(rank='same')
    for queue in data['queues']:
        dot.node(queue["name"], label=get_queue_label(queue), shape='box')

with dot.subgraph() as sub:
    sub.attr(rank='same')
    for consumer in data['consumers']:
        dot.node(consumer["name"], label=get_consumer_label(consumer), shape='box')
        dot.edge(consumer["source"], consumer["name"])

# Render the graph to a file
dot.render(data["server_metadata"]["name"], cleanup=True)
