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
- [x] JQ transformations
- [ ] Streaming will not be supported: it doesn't match the scheduling mechanism


### JQ setup

When working in VS Code, you need to set the environment variables for JQ:

```
    "rust-analyzer.server.extraEnv": {
        "JQ_LIB_DIR": "/usr/local/lib",
        "JQ_INCLUDE_DIR": "/usr/local/include"
    },
```

Outside of VS Code, you can set the environment variables directly:

```
export JQ_LIB_DIR=/usr/local/lib
export JQ_INCLUDE_DIR=/usr/local/include
```

I installed JQ from source. It's not on Homebrew.

`jq-sys` has a `bundled` feature but it didn't work for me on ARM.

To try a complex transform (if you have `cargo --example serving` running),

```
curl -X POST http://localhost:3000/execute \
  -H "Content-Type: application/json" \
  -d @tests/resources/complex_transform.json
```
