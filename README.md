# geo-dht
> status: experimental

Results of our applied research on geo-routing / -hashing in peer to peer systems.
This repo provides two DHT implementations with a 2D topology:

- 2D-CAN: DHT with a 2D grid-based topology.
  Fully functional, though no geohashing is implemented (unclear how this would be meaningful apart from a translation of geographic coordinates to keyspace coords..).
  Currently, nodes only know about their direct neighbours.

- 2D-Kademlia: DHT with XOR-metric based routing.
  Compared to Kademlia, the address space is twice as deep to accomodate for spatial addressing:
  Content- & node-addressing works through a [geohash] forming a Z-order curve, allowing to infer spatial relationships between nodes.

Originally, the plan was to compare the routing of both implementation through simulation in [The ONE]; but we're lacking time for this.

[The ONE]: https://github.com/akeranen/the-one

## usage
To start the first Node in a DHT:
```sh
python app.py
```

To start a new node and join it to an existing DHT:
```sh
python app.py {entry_port}
```
...where `{entry_port}` is a port on localhost with another running Node.

> Currently nodes only communicate whithin `localhost`. It should be easy to generalize to full IPs with some small modifications.

### Valid commands
* `GET {key}`
* `PUT {key} {value}`
* `STATE`
