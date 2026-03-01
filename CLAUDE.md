# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Rust-based graph database system that implements a sophisticated LRU cache with persistence for managing graph data in DynamoDB. The project features:

- **Concurrent data processing** using Tokio async runtime with configurable parallelism
- **LRU cache with persistence** that synchronizes access across multiple tasks
- **Graph data management** with overflow blocks (OvBs) for scalable edge storage
- **Reverse edge tracking** for bidirectional graph traversal
- **Performance monitoring** via event statistics collection

The system reads parent-child graph relationships from MySQL, propagates child scalar data to parent nodes, and persists everything to DynamoDB with automatic cache eviction and persistence management.

## Build and Run Commands

### Building the Project

```bash
# Build the entire workspace
cargo build

# Build in release mode
cargo build --release

# Build specific crate
cargo build -p lrucache
cargo build -p event_stats
```

### Running Tests

```bash
# Run all tests
cargo test

# Run tests for specific crate
cargo test -p lrucache
cargo test -p event_stats

# Run a specific test
cargo test <test_name>
```

### Running the Application

The application requires several environment variables to be set:

```bash
# Required environment variables:
export MYSQL_HOST="<mysql-host>"
export MYSQL_USER="<mysql-user>"
export MYSQL_PWD="<mysql-password>"
export MYSQL_DBNAME="<database-name>"
export MAX_SP_TASKS="<max-parallel-tasks>"
export MAX_PERSIST_TASKS="<max-persist-tasks>"
export GRAPH_NAME="<graph-name>"
export LRU_CAPACITY="<lru-cache-capacity>"

# Run the application
cargo run

# Run in release mode
cargo run --release
```

## Architecture

### Workspace Structure

This is a Cargo workspace with two main crates:

1. **Root crate (`sp`)**: The main application (src/main.rs)
   - Located in `src/` directory
   - Depends on `lrucache` crate

2. **`lrucache` crate**: Generic LRU cache library
   - Located in `cache/` directory
   - Provides `Cache<K,V>` with persistence support
   - Exports `event_stats` crate publicly

3. **`event_stats` crate**: Performance monitoring library
   - Located in `cache/event_stats/` directory
   - Tracks operation durations and channel wait times

### System Architecture Diagram

The LRU cache system follows a layered architecture with clear separation between application code and cache infrastructure:

```
┌─────────────────────────────────────────────────────┐
│         Database Load Application                    │
│                  (application code)                  │
└──────────┬──────────────┬──────────────┬────────────┘
           │              │              │
           ▼              ▼              ▼
    ┌──────────┐   ┌──────────┐   ┌──────────┐
    │  Load    │   │  Load    │   │  Load    │
    │  Task    │   │  Task    │   │  Task    │
    └─────┬────┘   └─────┬────┘   └─────┬────┘
          │ get          │ get          │ get
          │         Flush│              │
          ▼              ▼              ▼
    ┌─────────────────────────────────────────────┐
    │              Cache                          │◄──── Confirm
    │           (cache code)                      │
    └──┬──────────┬──────────┬──────────┬────────┘
       │          │          │          │
       │ Flush    │ Complete │Attach    │IsPersist
       │          │          │Move      │
       ▼          ▼          ▼          ▼
    ┌─────────────────────────────────────────────┐
    │           LRU Service                       │──── Read ──►
    │           (cache code)                      │
    └──────────────────┬──────────────────────────┘
                       │ Submit
                       ▼
    ┌─────────────────────────────────────────────┐
    │         Persist Service                     │◄──── Completed
    │           (cache code)                      │
    └──┬──────────────┬──────────────┬───────────┘
       │ Shutdown     │ Persist      │ Persist
       │              │              │
       ▼              ▼              ▼
    ┌──────────┐   ┌──────────┐   ┌──────────┐
    │ Persist  │   │ Persist  │   │ Persist  │
    │  Task    │   │  Task    │   │  Task    │
    └─────┬────┘   └─────┬────┘   └─────┬────┘
          │              │              │
          │ Persist      │ Persist      │ Persist
          ▼              ▼              ▼
    ┌─────────────────────────────────────────────┐
    │              Database                       │
    │            (DynamoDB)                       │
    └─────────────────────────────────────────────┘
```

#### Component Interactions

**Application Layer (Application Code)**:
- **Database Load Application**: Main application that orchestrates data loading
- **Load Tasks**: Multiple concurrent tasks (up to `MAX_SP_TASKS`) that process parent nodes
  - Call `cache.get()` to retrieve/create cache entries
  - Call `cache.flush()` during graceful shutdown

**Cache Layer (Cache Code)**:
- **Cache**: Central coordination point with state management
  - API: `get()`, `flush()`, `unlock()`
  - Tracks states: `inuse`, `persisting`, `loading`
  - Sends to LRU: `Attach`, `MoveToHead`, `Shutdown`, `Flush`
  - Receives from Persist: `Confirm` (persistence complete)
  - Queries Persist: `IsPersist` (check if key is being persisted)

- **LRU Service**: Manages cache eviction policy
  - Receives: `Attach` (new entry), `MoveToHead` (cache hit), `Flush` (shutdown)
  - Maintains doubly-linked list for LRU ordering
  - On capacity exceeded: identifies tail entry for eviction
  - Sends to Persist: `Submit` (request persistence before eviction)
  - Queries Cache: `Read` (check entry state before eviction)

- **Persist Service**: Manages asynchronous database writes
  - Maintains pool of persist tasks (up to `MAX_PERSIST_TASKS`)
  - Receives: `Submit` (from LRU), `Shutdown` (graceful shutdown)
  - Distributes work to available persist tasks via channel
  - Receives: `Completed` (from persist tasks)
  - Sends to Cache: `Confirm` (persistence done, update state)

**Database Layer (Application Code)**:
- **Persist Tasks**: Worker tasks that execute database writes
  - Receive persistence requests via channel
  - Execute `Persistence<K,D>::persist()` trait implementation
  - Write to DynamoDB (batch writes, conditional updates)
  - Report `Completed` back to Persist Service

#### Communication Patterns

- **Channels**: All inter-component communication uses Tokio channels (mpsc, broadcast)
- **API Calls**: Direct function calls for synchronous operations (within Mutex locks)
- **Coordination**: Broadcast channels synchronize state transitions across tasks
- **Backpressure**: Bounded channels and semaphores limit concurrent operations

#### Graceful Shutdown Flow

1. Application calls `cache.flush()` on all load tasks
2. Cache sends `Flush` to LRU Service
3. LRU Service submits all entries for persistence
4. Persist Service waits for all persist tasks to complete
5. Cache confirms all entries persisted before shutdown

### Core Components

#### Cache System (`cache/src/lib.rs`)

The `Cache<K,V>` is the central data structure providing:

- **Thread-safe concurrent access** via Arc<Mutex<InnerCache>>
- **State tracking**: Three HashMap states track which entries are `inuse`, `persisting`, or `loading`
- **Broadcast channels** for coordinating state transitions between tasks
- **Two background services**: LRU service (eviction) and Persist service (database writes)

Key traits:
- `Persistence<K,D>`: Implement this to define how to persist cache values to database
- `NewValue<K,V>`: Implement this to define how to create new cache values

#### LRU Service (`cache/src/service/lru.rs`)

Manages cache eviction using a doubly-linked list structure stored in a HashMap:

- **Attach operation**: Adds new entries to the head of the LRU list
- **MoveToHead operation**: Updates access order on cache hits
- **Eviction**: When capacity is exceeded, tries to evict from tail (only if not `inuse`)
- **Flush operation**: Persists all entries and waits for completion before shutdown

#### Persist Service (`cache/src/service/persist.rs`)

Handles asynchronous persistence of cache entries:

- **Multiple persist tasks**: Configurable parallelism for database writes
- **Pending queue**: Manages entries waiting for available persist tasks
- **Coordination with cache**: Updates cache state when persistence completes

#### Main Application Flow (`src/main.rs`)

1. **Initialization**: Loads config from environment, creates DynamoDB and MySQL clients
2. **Type loading**: Fetches graph schema/types from DynamoDB
3. **Service startup**: Starts retry service, stats service, and cache (which starts LRU and persist services)
4. **MySQL query**: Loads all parent-child relationships into memory
5. **Parallel processing**: Spawns tasks (up to MAX_SP_TASKS) to process each parent node:
   - Queries child scalar data from DynamoDB
   - Aggregates propagated scalars into parent edge attributes
   - Handles overflow blocks when data exceeds embedded limits
   - Writes to DynamoDB in batches of 25
   - Adds reverse edges to child nodes via cache
6. **Graceful shutdown**: Waits for tasks, flushes cache, shuts down services

#### Graph Data Model

**Overflow Block (OvB) Strategy**:
- Parent edges embed up to `EMBEDDED_CHILD_NODES` (default: 4) child references directly
- When exceeded, creates overflow blocks (up to `MAX_OV_BLOCKS`, default: 5)
- Each overflow block contains batches of size `OV_MAX_BATCH_SIZE` (default: 160)
- Uses round-robin allocation across overflow blocks for parallelism

**Reverse Edges**:
- Child nodes maintain reverse edges back to parents in sortkey format: `R#<parent-type>#:<edge-attr>`
- Managed via `RKey` (src/rkey.rs) and `RNode` (src/node.rs)
- Cache prevents duplicate writes and coordinates concurrent updates

### Data Schema Example: Parent-Child with Scalar Predicates

This example demonstrates GoGraph/DynamoDB design patterns for a Manager-Employee relationship.

#### Type Definitions

**Employee (Child) Type**:
- Scalars: Age (Ag), JobTitle (JT), YearsExperience (YE), Salary (Sa)
- UID Predicate: WorksFor (WF) → Manager

**Manager (Parent) Type**:
- Scalars: Age (Ag), JobTitle (JT), YearsExperience (YE), Salary (Sa)
- UID Predicate: Manages (Mg) → Employee

#### Sort Key Patterns

```
# Node scalar data (system partition "A#", data partition "A#")
A#A#T                    - Type attribute
A#A#:Ag                  - Age scalar
A#A#:JT                  - JobTitle scalar
A#A#:YE                  - YearsExperience scalar
A#A#:Sa                  - Salary scalar

# UID predicate edges (system partition "A#", data partition "G#")
A#G#:Mg                  - Manager's "Manages" edge (Employee UUIDs)
A#G#:WF                  - Employee's "WorksFor" edge (Manager UUIDs)

# Propagated child data at parent
A#G#:Mg|Ag              - Propagated Age from Employees
A#G#:Mg|JT              - Propagated JobTitle from Employees
A#G#:Mg|YE              - Propagated YearsExperience from Employees
A#G#:Mg|Sa              - Propagated Salary from Employees

# Overflow blocks (when Manager has >4 Employees)
A#G#:Mg#:1              - Overflow block 1
A#G#:Mg#:2              - Overflow block 2
```

#### Example DynamoDB Items

**Manager Node with Embedded Employees**:
```json
{
  "PKey": "<manager-uuid-binary>",
  "SortK": "A#G#:Mg",
  "Nd": ["<emp1-uuid>", "<emp2-uuid>", "<emp3-uuid>"],
  "LN": [28, 35, 42],
  "LS": ["Software Engineer", "Senior Engineer", "Lead Engineer"],
  "XN": [5, 10, 15],
  "LN#2": [75000, 95000, 110000]
}
```

**Attribute Mapping**:
- `Nd` - Employee UUIDs (child references)
- `LN` - Ages (corresponds to Ag scalar)
- `LS` - Job Titles (corresponds to JT scalar)
- `XN` - Years Experience (corresponds to YE scalar)
- `LN#2` - Salaries (corresponds to Sa scalar)

**Data Propagation Benefits**:
1. **Single-node query**: Manager and all employee data retrieved in one DynamoDB query
2. **Positional alignment**: Employee at `Nd[i]` has Age at `LN[i]`, JobTitle at `LS[i]`, etc.
3. **Performance**: ~55-104 μs per employee (based on GoGraph benchmarks)
4. **Scalability**: Overflow blocks partition large teams across multiple items

### Key Files

- `src/main.rs` - Application entry point, orchestrates the entire pipeline
- `src/node.rs` - `RNode` struct for reverse edge data and OvB metadata management
- `src/rkey.rs` - `RKey` cache key for reverse edges (child UUID + reverse SK)
- `src/types/mod.rs` - Graph type system and DynamoDB attribute name constants
- `src/types/block.rs` - DynamoDB data item structures and conversion logic
- `src/service/retry.rs` - Handles failed DynamoDB writes with retry logic
- `cache/src/lib.rs` - Generic cache implementation with state management
- `cache/src/service/lru.rs` - LRU eviction service
- `cache/src/service/persist.rs` - Persistence service with task pool

### Important Constants

Defined in `src/main.rs`:
- `DYNAMO_BATCH_SIZE` (25): DynamoDB BatchWriteItem limit
- `EMBEDDED_CHILD_NODES` (4): Child nodes embedded in parent edge before overflow
- `MAX_OV_BLOCKS` (5): Maximum overflow blocks for parallel reads
- `OV_MAX_BATCH_SIZE` (160): Items per overflow batch
- `OV_BATCH_THRESHOLD` (4): Initial batches before creating new overflow block

## Development Notes

### Working with the Cache

When using `Cache::get()`:
1. Returns `CacheValue::New(arc_value)` for cache misses (value is locked)
2. Returns `CacheValue::Existing(arc_value)` for cache hits
3. **Always call `cache.unlock(key, task)` when done** to release `inuse` and `loading` states
4. The returned `Arc<Mutex<V>>` must be locked to access the value
5. Cache guard is released before value guard to maximize concurrency

### Persistence Implementation

To add persistence for a new type:
1. Implement the `Persistence<K,D>` trait with async `persist()` method
2. Implement `NewValue<K,V>` trait to create empty values for new keys
3. The persist method receives the database handle and waits recorder
4. Coordinate with cache state to prevent race conditions during eviction

### Event Statistics

The `event_stats::Waits` recorder tracks timing for:
- Mutex acquisition (LRU, Cache)
- Channel operations (send/receive waits)
- Cache operations (get, evict, persist)
- DynamoDB operations (GetItem, UpdateItem, BatchWriteItem)

Access via `waits.record(Event::EventName, duration).await`

### Tokio Concurrency Patterns

- **Task coordination**: Uses mpsc channels for task completion signaling
- **State synchronization**: Broadcast channels notify waiters when states transition
- **Selective receive**: `tokio::select!` with `biased` for deterministic priority
- **Graceful shutdown**: Broadcast channel signals all services to stop
- **Bounded parallelism**: Limits concurrent tasks to prevent resource exhaustion

### DynamoDB Integration

- **Batch writes**: Accumulates up to 25 items before calling `batch_write_item()`
- **Unprocessed items**: Retried via dedicated retry service with 2-second delay
- **Conditional updates**: UpdateItem with SIZE() checks for overflow batch limits
- **Table name**: Hardcoded as "RustGraph.dev.11" in main.rs:199
