# Telemetry Aggregator: Scaling and Production Design Note 

This document outlines the scaling characteristics, performance profile, and proposed deployment strategy for the Telemetry Aggregator service.

## 1. Phase 0: Single Worker (Initial Solution)

The initial deployment runs as a single, asynchronous worker process. The architecture utilizes a Read-Copy-Update (RCU) pattern combined with fully asynchronous I/O and non-blocking parsing.

### Metric Characteristics

| Metric             | Characteristic            | Rationale                                                                    |
|--------------------|---------------------------|-------------------------------------------------------------------------------|
| API Query (Reader) | CPU-Bound (Instantaneous) | Direct dictionary lookup (`telemetry_store.get_data()`). Latency is sub-ms. |

The architecture is currently optimized for maximum throughput on a single Uvicorn worker by ensuring cooperative multitasking is never broken by synchronous operations.

## 2. Phase 1: Vertical Scaling (Maximizing Cores on a Single Machine)

To handle increased API query volume and overcome the single-worker performance limit imposed by the Python GIL, the first step is to scale vertically by introducing multiple worker processes (e.g., via Gunicorn) to maximize the use of all available CPU cores on the single host machine.

The goal is to dedicate machine resources optimally:

- If a machine has $N$ cores, we run $N$ total worker processes.
- **1 Worker (The Writer):** Only one worker is designated as the Leader. This worker runs the ingestion_task, fetches data, parses it, and updates the shared in-memory data snapshot (the Write operation).
- **$N-1$ Workers (The Readers):** The remaining $N-1$ workers are dedicated exclusively to serving API requests (the Read operations).

### Core-Efficient RCU Synchronization

To ensure the $N-1$ reader workers always have the freshest data copy:

1. **Shared Snapshot:** The Leader (Writer) broadcasts the newly ingested data snapshot to the other $N-1$ workers using Inter-Process Communication (IPC).
2. **Lock-Free Reads:** Each of the $N-1$ reader workers stores a copy of this snapshot in its local memory (an RCU update). This spreads the data across the multiple workers' local caches, significantly boosting read performance.
3. **Performance:** Because they use the RCU pattern, every API query (read) hits this local, in-memory snapshot instantly, making all reads consistently fast and lock-free after the initial load.

This strategy ensures that every core is busy serving instantaneous read queries, maximizing the single machine's capacity before we need to scale horizontally.

### Throughput Ceiling Calculation (Expected Throughput Limits)

We can estimate the theoretical maximum read throughput (Queries Per Second, QPS) for a single host by defining key assumptions:

| Assumption              | Value | Description                                                                      |
|-------------------------|-------|----------------------------------------------------------------------------------|
| Cores ($N$)             | $8$   | Standard cloud compute instance size (e.g., c5.2xlarge).                         |
| RCU Read Latency ($L_R$) | $0.00005\text{s}$ (50 $\mu s$) | Estimated time for a complete, lock-free RCU lookup and API response overhead. |
| Ingestion Frequency ($F_I$) | $\approx 2\text{s}$ | Governed by the network fetch latency (external dependency).                     |

Maximum Read QPS:

$$\text{QPS}_{\text{Max}} = N \times \left( \frac{1}{L_R} \right) = 8 \times \left( \frac{1}{0.00005\text{s}} \right) = 160{,}000 \text{ QPS}$$

**Ingestion (Write) Impact:** Since the ingestion writer is I/O bound (mostly waiting) and the readers use lock-free RCU copies, the write activity does not significantly degrade the read QPS. The maximum sustained throughput for reads is therefore constrained only by the available CPU cores and the fast RCU lookup time.

## 3. Phase 2: Horizontal Scaling (Multi-Node Deployment)

Once the single host machine's CPU or RAM limits are reached, the system must scale horizontally across multiple nodes. This requires transitioning from the in-memory RCU model to a fully distributed system utilizing an external shared data store to separate compute and storage.

## 4. Ultimate Performance Ceiling: Transition to Native Code

If, after all Python-based optimizations are exhausted, the core aggregation logic remains persistently CPU-bound due to the Python GIL, the final solution is migrating those critical components to native code (C++). This provides significantly faster execution and eliminates GIL constraints, allowing for true parallel processing of data aggregation.


