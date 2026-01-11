# RAFT - A Go Implementation of the Raft Consensus Algorithm

This repository provides a Go implementation of the Raft consensus algorithm, a fault-tolerant protocol for managing a replicated log and building strongly consistent distributed systems.

Raft allows a cluster of servers to agree on a sequence of commands despite crashes, message loss, reordering, or network partitions, as long as a majority of servers remain available.

This implementation follows the design described in:

> *In Search of an Understandable Consensus Algorithm*
> Diego Ongaro & John Ousterhout (2014)

---

## Core Raft Features

Raft decomposes consensus into a small set of well-defined mechanisms that together provide safety and fault tolerance.

---

### 1. Replicated State Machine

Clients submit commands that are appended to a replicated log.
Committed entries are applied in order on all servers, ensuring deterministic state.

**Guarantee:** All non-faulty servers converge to the same state.

---

### 2. Strong Leadership Model

Raft enforces a single leader per term.

* Only the leader accepts client requests and replicates log entries
* Followers are passive
* Candidates exist only during elections

**Guarantee:** At most one leader operates in a given term.

---

### 3. Terms (Logical Time)

Time is divided into monotonically increasing terms.

* Servers track `currentTerm`
* RPCs include term numbers
* Servers step down when observing a higher term

This prevents stale leaders from corrupting state.

---

### 4. Leader Election

Leaders are elected via majority voting with randomized timeouts.

**Guarantees:**

* Election Safety: only one leader per term
* Only up-to-date servers can become leader

---

### 5. Heartbeats

Leaders send periodic empty `AppendEntries` RPCs to:

* Maintain leadership
* Prevent unnecessary elections
* Propagate commit progress

---

### 6. Log Structure & Replication

Each server maintains an append-only log of `(command, term, index)` entries.

* Leaders replicate entries using `AppendEntries`
* Log inconsistencies are detected and repaired automatically

**Invariants:** Leader Append-Only, Log Matching Property

---

### 7. Commitment Rules

An entry from the current term is committed once replicated on a majority.

* Leaders advance `commitIndex`
* Followers learn commits via heartbeats
* Committed entries are applied in order

**Guarantee:** Committed entries are never lost.

---

### 8. State Machine Application

Servers apply committed entries sequentially and deliver them via `ApplyMsg`.

**Guarantee:** No two servers apply different commands at the same index.

---

### 9. Election Restriction (Leader Completeness)

Servers vote only for candidates with up-to-date logs.

**Guarantee:** All future leaders contain all committed entries.

---

### 10. Fault Tolerance & Persistence

Raft tolerates crashes, partitions, and message loss as long as a majority is available.

Critical state (`currentTerm`, `votedFor`, logs, snapshots) is persisted to survive crashes.

---

### 11. Log Compaction (Snapshots)

Snapshots discard old log entries while preserving correctness.

* Services create snapshots
* Leaders send snapshots via `InstallSnapshot` when needed

---

## Repository Structure

```
├── labgob/        # Encoding helpers
├── labrpc/        # RPC network simulation layer
├── raft1/         # Core Raft implementation
├── raftapi/       # Shared Interface definitions
├── tester1/       # Test harness
├── go.mod
└── go.sum
```
