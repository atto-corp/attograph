attograph is a langgraph-esque graph workflow execution layer built on top of the Rust library implementation of BullMQ.

Basic concepts:
- A graph, consisting of nodes and connections between them (DAG)
  - Input is a json, output is a json (if large files are written, they are returned by identifier in the output json)
  - Can be stored as a graph.json file, referencing nodes via string name tags without knowing the contents/definition of the nodes).
  - Or can be stored in the Graphs data storage collection.
  - Versions identified by a unique hash of the graph.json
  - Version hashes can additionally be assigned string names like 'myrelease' or 'myrelease:1.0.2'
  - Graph definition only contains string tags of nodes, which are resolved to graph definitions separately
- GraphExecution = a historical execution of a graph.
  - Contains the full input json of the overall graph
  - NodeExecution = a historical execution of a node. Contains the entire input and output values along with start and stop timestamp