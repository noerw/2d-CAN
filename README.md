python node.py
Ask for own_port
Init node with own_port
Keyspace = (0, 1)
Listen on that own_port
Wait for input

python node.py {entry_port}
Ask for a own_port
Init node with own_port
Ask the entry_port for our keyspace
Listen to this own_port
Wait for input

# it prints the port, stays open, is able to respond to queries

# Boot up a node with an address
# it asks for keyspace from the process at that port, waits for input
