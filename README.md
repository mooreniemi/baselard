```
[]++++||=======>
```

# Baselard

Baselard is a library for building and executing directed acyclic graphs (DAGs) of components.

## Features:

- [x] Define a DAG in JSON
- [x] Execute a DAG from an intermediate representation
- [x] Validate configuration of components
- [x] Handle errors in components
- [x] Time out components
- [x] Register custom components
- [x] Type guarantees on inputs and outputs of components
- [x] But also... allow "wildcard" inputs and outputs in components via JSON
- [x] Types include `Union` types to allow flexibility
- [x] Cache results of components
- [x] Keep a request history of components for replay
- [x] Automatically handle parallel nodes
- [ ] Streaming will not be supported: it doesn't match the scheduling mechanism