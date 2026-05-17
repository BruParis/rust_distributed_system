# rust-distributed-system

A personal implementation of the [Jepsen-io Maelstrom](https://github.com/jepsen-io/maelstrom/maelstrom/tree/main) distributed systems challenges in Rust, following [Jon Gjengest's video walkthrough](https://www.youtube.com/watch?v=gboGyccRVXI&t=10337s).

## Progress

| Chapter | Description | Status |
|---------|-------------|--------|
| 1 | Echo server | Done |
| 2 | Broadcast (single node) | Done |
| 3 | Broadcast with fault tolerance (gossip + retry) | Done |
| 4 | Grow-only Set (CRDT) | Done |
| 5.1 | Single-node transactions | Done |
| 5.2 | Multi-node transactions (read-committed) | Done |
| 5.3 | Totally-ordered transactions | Done |
| 5.4 | Efficient transactions | Missing |
| 6 | Raft consensus | Not started |

---

## Architecture

The project is a Cargo workspace with one library crate and six binary targets.

```
src/
├── lib.rs              # module re-exports
├── output.rs           # mutex-protected stdout/stderr writer
├── echo/               # Ch.1 — echo protocol
├── broadcast/          # Ch.2-3 — gossip broadcast with retry
├── crdt/               # Ch.4, 5.1-5.3 — GSet and PNCounter CRDTs
├── datomic/            # Ch.5 — list-append transactions over lin-kv
├── raft/               # Ch.6 — stub only
└── bin/                # binary entry points (one per challenge)
    ├── echo_server.rs
    ├── broadcast.rs
    ├── gset.rs
    ├── pn_counter.rs
    ├── datomic.rs
    └── raft.rs
```

### Threading model

All nodes follow the same pattern:

1. **Main thread** reads lines from stdin and sends each raw JSON string over an MPSC channel.
2. **Dispatcher** reads from the channel and spawns a handler thread per message.
3. **Background threads** (where applicable):
   - Broadcast: one retry thread per in-flight RPC
   - CRDT: a periodic replication thread (10 ms interval)

Output is serialised through a global `Mutex` so stdout lines never interleave.

---

## Modules

### `echo`
Stateless request-reply server. Receives `echo` messages, responds with `echo_ok`.

### `broadcast`
Gossip-based broadcast. Each node keeps a `HashSet` of seen values and forwards every new value to all neighbours. Unacknowledged sends are retried by a dedicated background thread per RPC. Topology is learned from a `topology` message sent by the harness.

### `crdt`
Generic CRDT node parameterised over `CrdtTrait`. Two implementations:

- **GSet** — grow-only set. Merge = union.
- **PNCounter** — PN-Counter with per-node positive/negative maps. Merge = component-wise max of absolute values.

Replication sends the full local state every 10 ms to all neighbours.

### `datomic`
Transactional list-append over Maelstrom's `lin-kv` service.

Key abstractions:
- **Thunk** — a lazily-loaded value stored in lin-kv. Serialises as its ID string; fetched on demand.
- **Promise** — a `Condvar`-based future that blocks until a lin-kv reply arrives (25 ms timeout).
- **Transaction loop** — reads the root map, applies operations, saves new thunks, then CAS the root. Retries on CAS conflict.

Data layout: `root` key → `ThunkMap` (key → thunk ID) → `ThunkValues` (list of appended numbers).

### `raft`
Skeleton. Stores key-value pairs in a `HashMap` and handles read/write/CAS messages, but has no log replication or leader election. Not functional as a consensus implementation.

---

## Dependencies

| Crate | Use |
|-------|-----|
| `serde` + `serde_json` | Message serialisation |
| `maelstrom-common` | Shared Maelstrom protocol types |
| `crossbeam` | Concurrency utilities (channels, scoped threads) |

---

## Installing Maelstrom

Requires Java (JDK 11+), optionally `graphviz` and `gnuplot` for result visualisation.

```bash
cd ..
wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2
tar -xjf maelstrom.tar.bz2 && rm maelstrom.tar.bz2
```

Expected layout:
```
parent-dir/
├── maelstrom/
└── rust_distributed_system/
```

Results are written to `maelstrom/store/latest/index.html` after each run.

---

## Running the tests

```bash
make echo
make broadcast
make broadcast-partition
make gset
make gset-partition
make pncounter
make txn
make txn-multi
```

For lower latency, override the `BIN` variable to use the release build:

```bash
make echo BIN=target/release
```
