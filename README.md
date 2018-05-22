# SimpleDynamo

The purpose of this project is to implement simplified version of Amazon Dynamo key-value storage system. 
We implement partitioning, replication and failure handling.

The goal is to implement a distributed key-value storage system that provides both availability and linearizability. 
Our implementation performs successful read and write operations even in the presence of failures.

In short, we implement the following things
• Data replication
• Data partitioning
• Handle node failures while continuing to provide availability and linearizability.

To implement linearizability, this project uses chain replication. 
Key-values are written in the head of the chain and read from the tail always. This ensures linearizability across all nodes.
Replication is done in the two following successors. If one of these nodes is down, we do not add new node replica like in 
Dynamo.
