# network-telemetry-aggregator

## 🌟 Project Summary

This project implements a high-performance core component of a network telemetry aggregation system, inspired by NVIDIA's UFM/Fabric Manager. The system is designed for **low-latency, real-time query responses** and guarantees data freshness and **non-blocking reads**, leveraging an optimized concurrency pattern.

The solution is implemented as two Python-based REST servers: a simulated **Telemetry Data Server** (Producer) and the main **Metrics Aggregation Server** (Consumer/API).

---

## 📐 System Architecture and Design

### Core Architecture

The system uses a **monolithic, single-process design** for the Metrics Server (`aggregator_server.py`) to maximize efficiency and minimize network overhead between components.

1. **Telemetry Data Server (Producer):** Runs on port `9001`. Simulates 50 fabric switches, periodically generating a new data snapshot and exposing it as a single CSV matrix at `/counters`.
2. **Metrics Aggregation Server (Consumer/API):** Runs on port `8080`.
   * **Ingestion Task:** An **asynchronous background task** polls the Producer every 10 seconds.
   * **RCU Store:** Uses the Read-Copy-Update pattern for its in-memory data store.

### High-Performance Concurrency (Read-Copy-Update - RCU)

The key to achieving the assignment's requirement for **non-blocking queries** (i.e., **zero degradation of performance under simultaneous queries**) is the **Read-Copy-Update (RCU)** synchronization pattern.

* The internal store has a **public read copy** and a **private write copy**.
* The Ingestion Task (Writer) performs all heavy operations (HTTP fetch, CSV parsing) **outside of any lock**.
* The final update is a single, instantaneous **reference swap** (`self.current_data = new_snapshot`), protected only briefly by an `asyncio.Lock`.
* **Result:** API readers access the public copy **without needing a lock**, guaranteeing **zero-blocking reads** and optimal query performance.

---

## 🚀 Running the System

### **Prerequisites**

The following Python packages are required (install them in a virtual environment):
```bash
pip install fastapi uvicorn httpx pandas
```

### **Running Instructions**

The two components must be run in separate terminals:

1. **Start the Telemetry Generator (Producer):**
   * Runs on http://127.0.0.1:9001

   ```bash
   python generator.py
   ```

   * **Edge Case Testing:** To simulate ingestion failures, you can run the Generator in a specific mode (e.g., `python generator.py ERROR_500`) or call the `/counters` endpoint with a mode query parameter (e.g., `http://127.0.0.1:9001/counters?mode=CORRUPT`).

2. **Start the Metrics Aggregator (Consumer/API):**
   * Runs on http://127.0.0.1:8080
   * The background task will automatically begin polling the Generator every 10 seconds.

   ```bash
   python aggregator_server.py
   ```

### **Testing the API Endpoints**

| Endpoint | Method | Purpose | Example Query |
| :---- | :---- | :---- | :---- |
| /telemetry/ListMetrics | GET | Fetch all current metrics for all 50 switches. | http://127.0.0.1:8080/telemetry/ListMetrics |
| /telemetry/GetMetric | GET | Fetch a specific metric value for a given switch ID. | http://127.0.0.1:8080/telemetry/GetMetric?switch_id=SW-05&metric_name=Latency_Avg_uSec |

---

## 📊 Performance and Observability

### **Performance Statistics Reporting**

The system measures and logs performance in two key areas:

1. **Data Freshness (Writer Performance):** The background task logs the total time taken for the **Ingestion Cycle** (fetch, parse, RCU swap). This directly tracks the system's ability to maintain data freshness.
2. **Query Latency (Reader Performance):** API endpoints rely on the RCU pattern and **O(1) dictionary lookups**, ensuring consistently low latency.

### **Performance Benchmarks (Expected/Simulated Results)**

| Metric | Measured Component | Expected Performance | Reason |
| :---- | :---- | :---- | :---- |
| **Ingestion Cycle Time** | Writer (Total time) | **50 - 150 milliseconds** | Dominated by network latency (HTTP fetch). CPU processing (parsing/swap) is extremely fast. |
| **GetMetric Latency** | Reader (Single lookup) | **< 1 millisecond** (sub-millisecond) | RCU ensures a lock-free, O(1) dictionary lookup for optimal speed. |
| **Query Contention** | Reader Blocking | **Zero Blocking** | The RCU atomic swap pattern eliminates contention between concurrent API readers and the background ingestion task. |

---

## ⚙️ Limitations & Suggestions for Improvement (Scalability, Throughput, Fault-Tolerance)

The current monolithic design is highly performant but has inherent limitations in a large-scale production environment.

### **1. Scalability and Persistence**

| Limitation | Solution (Advanced Pattern) | Impact on System |
| :---- | :---- | :---- |
| **Single Point of Failure / Persistence** | Implement an external, highly available data store (e.g., **Redis** or a Time-Series Database). | Allows API instances to be scaled horizontally (run on multiple machines) while sharing the same data state. Adds **persistence** (data survives API restarts). |
| **Limited Parallelism / CPU Contention** | Introduce **Process Sharding (Data Partitioning)** based on CPU cores. | **Highest Throughput:** By forking multiple Python processes, each handling a dedicated range of switch IDs, the system bypasses the Python **Global Interpreter Lock (GIL)**. This maximizes **Cache Locality** and achieves true parallel processing. |

### **2. Ingestion Throughput Optimization**

| Limitation | Solution (Advanced Pattern) | Impact on System |
| :---- | :---- | :---- |
| **HTTP/TCP Overhead for Telemetry** | Replace the HTTP ingestion feed with a low-level, zero-copy mechanism like **RDMA (Remote Direct Memory Access)**. | **Ultra-Low Latency:** RDMA allows the switches to push data directly into the application's memory buffers, bypassing the target CPU and OS kernel entirely. This is crucial for maximizing throughput in high-volume fabric environments. |
| **Tightly Coupled Ingestion** | Introduce a high-throughput message queue (e.g., Kafka) between the generator and the aggregator. | **Decoupling and Resilience:** Decouples the two services, adds a durable buffer for data, and allows the system to handle sudden, massive bursts of telemetry data (back pressure). |
