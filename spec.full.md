attograph is a langgraph-esque graph workflow execution layer built on top of the Rust library implementation of BullMQ.

Basic concepts:
- A graph, consisting of nodes and connections between them (a DAG)
  - Input is a json, output is a json (if large files are written, they are returned by identifier in the output json)
  - Nodes are referenced only by string tags; concrete implementations are resolved separately from a NodeRegistry at enqueue time (tags must not contain ':')
  - Can be stored as a graph.json file, or in the Graphs data storage collection
  - Versions identified by a unique hash of the canonical graph json
  - Version hashes can additionally be assigned string names like 'myrelease' or 'myrelease:1.0.2'
  - Validation rejects empty names, empty/duplicate nodes, unknown edge endpoints, ':' in tags, and cycles
- Execution is scheduled as a BullMQ job per node, enqueued in topological (dependency) order
  - A node runs once all its predecessors have completed; it receives a NodeContext of { "input": graph_input, "state": { <predecessor>: <output> } }
  - A Retryable node error retries with exponential backoff (default 3 attempts); a Permanent error fails the node and the whole graph
  - The graph result is the merged map of end-node outputs once all end nodes complete
  - Execution and per-node histories are recorded (input/output, status, attempts, timestamps)
- Storage is pluggable via a Storage trait: MemoryStorage (in-process, default) or MongoStorage (optional 'mongo' feature), persisting graphs, versions, aliases, executions, and node-execution snapshots
- GraphExecution = a historical execution of a graph, with the full input json, output, status (pending/running/completed/failed/cancelled), error, and timestamps. NodeExecution = a historical execution of a node, with its entire input/output values, status, attempts, and start/stop timestamps.
