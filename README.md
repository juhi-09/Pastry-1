# Pastry
Implementation of Distributed De-centralized hash table.

A distributed hash table (DHT) is a class of a decentralized distributed system that provides a
lookup service similar to a hash table: (key, value) pairs are stored in a DHT, and any
participating node can efficiently retrieve the value associated with a given key. This implementation supports following 
major operations:
1) Join a new Node in the network.
2) Set the <key,value> pair
3) Get the value of key
4) Delete a node from Network

Command Execution Sequence - 

(1) make
(2) ./pastry
(3) port 5000
(4) create

// Join this new node to the first node using - 
// join <IP> <Port>

(5) join <IP> 5000 
(6) put <key> <value>
(7) get <key>
(8) quit
(9) shutdown

// To Print the tables

(10) lset
(11) nset
(12) routetable
(13) hashtable
