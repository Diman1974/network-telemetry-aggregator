import asyncio
import time
import httpx
import json
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from typing import Dict, Any, Optional

# Import the core non-blocking logic and utilities
from aggregator_utils import parse_csv_data, retry_with_backoff, logger

# --- Configuration ---
TELEMETRY_SOURCE_URL = "http://127.0.0.1:9001/counters"
INGESTION_INTERVAL_SECONDS = 10
HTTP_TIMEOUT_SECONDS = 5

# --- Data Store (RCU Implementation) ---

class AggregatorStore:
    """
    Read-Copy-Update (RCU) Store with integrated Observability Metrics.
    The RCU pattern ensures API reads are lock-free and instantaneous (Req 1).
    """
    def __init__(self):
        self.current_data: Dict[str, Any] = {}
        # Lock is only for the instantaneous pointer swap (Req 1)
        self.swap_lock = asyncio.Lock()
        
        # Observability Metrics (Req 3)
        self.metrics = {
            "ingestion_success_total": 0,
            "ingestion_failure_total": 0,
            "last_ingestion_latency_ms": 0.0, 
            "last_parse_latency_ms": 0.0,     # Core metric for Req 2 (CSV performance)
            "api_query_count": 0,
            "last_successful_ingestion_timestamp": 0, 
            "is_ready": False, # Readiness check status for /health
        }

    def get_data(self) -> Dict[str, Any]:
        """Non-blocking read operation (Req 1)."""
        # Reader always points to the current data structure, requiring no lock.
        return self.current_data

    async def _swap_data(self, new_snapshot: dict):
        """Performs the atomic RCU pointer swap and updates timestamp."""
        # The lock is held only for the duration of the pointer assignment.
        async with self.swap_lock:
            self.current_data = new_snapshot
            self.metrics["last_successful_ingestion_timestamp"] = int(time.time())

    def update_metrics(self, key: str, value: Any):
        """Helper to update internal metrics dictionary."""
        self.metrics[key] = value

    def get_metrics(self) -> Dict[str, Any]:
        """Provides a safe copy of the metrics."""
        return self.metrics

# --- FastAPI App Initialization ---

app = FastAPI(title="NVIDIA Telemetry Aggregator (RCU)", version="2.0")
telemetry_store = AggregatorStore()
# httpx.AsyncClient ensures network I/O is fully non-blocking (Req 1)
http_client = httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS)


# --- Worker Tasks ---

@retry_with_backoff
async def fetch_and_parse_telemetry() -> Optional[dict]:
    """
    Fetches raw CSV data from the generator and parses it.
    Uses retry with exponential backoff on network errors (Req 4).
    """
    # 1. Fetch data (Fully non-blocking due to httpx.AsyncClient, Req 1)
    fetch_start_time = time.time()
    response = await http_client.get(TELEMETRY_SOURCE_URL)
    fetch_end_time = time.time()

    # Raise for 4xx/5xx status codes to trigger the retry decorator
    response.raise_for_status() 

    # 2. Parse data (Req 2)
    parse_start_time = time.time()
    
    # parse_csv_data uses Python's built-in, lightweight csv module
    # to avoid blocking the event loop with heavy libraries (Req 2).
    try:
        new_snapshot = parse_csv_data(response.text)
    except ValueError as e:
        logger.error(f"Data validation/parsing failed: {e}. Snapshot is invalid.")
        telemetry_store.update_metrics(
            "ingestion_failure_total", telemetry_store.get_metrics()["ingestion_failure_total"] + 1
        )
        # Return None: RCU swap will be skipped on invalid data (Req 4)
        return None
        
    parse_end_time = time.time()
    
    # 3. Update Metrics (Req 2 & 3 - Latency Logging)
    telemetry_store.update_metrics(
        "last_ingestion_latency_ms", (fetch_end_time - fetch_start_time) * 1000
    )
    # THIS IS THE CRITICAL METRIC for CSV Performance (Req 2)
    telemetry_store.update_metrics(
        "last_parse_latency_ms", (parse_end_time - parse_start_time) * 1000
    )
    
    return new_snapshot


async def ingestion_task():
    """
    Main background task for continuous data ingestion.
    """
    logger.info("Starting ingestion background task...")
    
    # 1. Initial Load: Attempt to fetch and process the first snapshot
    initial_snapshot = await fetch_and_parse_telemetry()
    if initial_snapshot:
        await telemetry_store._swap_data(initial_snapshot)
        telemetry_store.update_metrics("is_ready", True)
        logger.info("Initial snapshot successfully loaded. Server is READY.")
    else:
        logger.error("Failed to load initial snapshot after all retries. Server starts in unready state.")

    # 2. Continuous Loop
    while True:
        start_time = time.time()
        
        try:
            new_snapshot = await fetch_and_parse_telemetry()
            
            if new_snapshot is not None:
                # RCU Swap (Req 4 - only update if snapshot is valid)
                await telemetry_store._swap_data(new_snapshot)
                
                # Structured Logging (Req 3)
                ingestion_duration = (time.time() - start_time) * 1000
                telemetry_store.update_metrics("ingestion_success_total", telemetry_store.get_metrics()["ingestion_success_total"] + 1)
                
                # Log a structured JSON object for observability
                logger.info(json.dumps({
                    "event": "RCU_Update_Success",
                    "total_duration_ms": f"{ingestion_duration:.2f}",
                    "fetch_duration_ms": f"{telemetry_store.metrics['last_ingestion_latency_ms']:.2f}",
                    "parse_duration_ms": f"{telemetry_store.metrics['last_parse_latency_ms']:.2f}", # Logged performance metric
                    "last_successful_ingestion_timestamp": telemetry_store.metrics["last_successful_ingestion_timestamp"],
                    "data_points": sum(len(v) for v in new_snapshot.values())
                }))

        except Exception:
            # Fatal error (e.g., generator permanently unreachable after all retries)
            # The ingestion_task ensures the old data snapshot is retained.
            pass

        # Pause until the next cycle (fully non-blocking)
        await asyncio.sleep(INGESTION_INTERVAL_SECONDS)


@app.on_event("startup")
async def startup_event():
    """Starts the background ingestion task on application startup."""
    app.state.ingestion_task = asyncio.create_task(ingestion_task())

@app.on_event("shutdown")
async def shutdown_event():
    """Closes the HTTP client and cancels the ingestion task."""
    app.state.ingestion_task.cancel()
    await http_client.aclose()


# --- API Endpoints ---
# Endpoints are fully non-blocking, using only lock-free RCU reads (Req 1)

@app.get("/health", 
         summary="Readiness and Liveness Check (Req 3)",
         response_model=dict)
async def health_check():
    """Returns 200 OK only after the initial snapshot is loaded (Readiness)."""
    is_ready = telemetry_store.get_metrics().get("is_ready", False)
    
    if not is_ready:
        return PlainTextResponse("Service Unavailable: Initial data loading.", status_code=503)
    
    return {"status": "OK", "message": "Server is fully operational and serving data."}


@app.get("/metrics", 
         summary="Prometheus Metrics Endpoint (Req 3)",
         response_class=PlainTextResponse)
async def prometheus_metrics():
    """Provides Prometheus-formatted metrics on ingestion health and performance."""
    metrics = telemetry_store.get_metrics()
    
    telemetry_store.update_metrics("api_query_count", metrics["api_query_count"] + 1)
    
    # Format the metrics into the Prometheus text format
    prometheus_output = [
        "# TYPE aggregator_ingestion_success_total counter",
        f"aggregator_ingestion_success_total {metrics['ingestion_success_total']}",
        
        "# TYPE aggregator_ingestion_failure_total counter",
        f"aggregator_ingestion_failure_total {metrics['ingestion_failure_total']}",
        
        "# TYPE aggregator_ingestion_latency_ms gauge",
        f"aggregator_ingestion_latency_ms {metrics['last_ingestion_latency_ms']:.3f}",
        
        "# TYPE aggregator_parse_latency_ms gauge",
        f"aggregator_parse_latency_ms {metrics['last_parse_latency_ms']:.3f}", # Exporting the performance metric
        
        "# TYPE aggregator_last_successful_ingestion_timestamp gauge",
        f"aggregator_last_successful_ingestion_timestamp {metrics['last_successful_ingestion_timestamp']}",
    ]
    
    return PlainTextResponse(content="\n".join(prometheus_output), status_code=200)


@app.get("/telemetry/ListMetrics", 
         summary="Fetch all current metrics for all switches.",
         response_model=dict)
async def list_metrics():
    """Retrieves the full telemetry snapshot from the RCU store (lock-free read)."""
    
    start_time = time.time()
    response_data = telemetry_store.get_data()
    end_time = time.time()
    
    latency_ms = (end_time - start_time) * 1000
    
    # Log API latency (Req 3)
    logger.info(json.dumps({
        "event": "API_Query_Latency",
        "endpoint": "/ListMetrics",
        "latency_ms": f"{latency_ms:.3f}",
        "data_count": len(response_data)
    }))
    
    return response_data

@app.get("/telemetry/GetMetric/{switch_id}/{metric_id}", 
         summary="Fetch a specific metric for a switch.",
         response_model=dict)
async def get_metric(switch_id: str, metric_id: str):
    """Retrieves a specific metric value from the RCU store."""
    
    start_time = time.time()
    data = telemetry_store.get_data()
    metric_value = data.get(switch_id, {}).get(metric_id)
    
    if metric_value is None:
        return PlainTextResponse("Metric not found", status_code=404) 

    end_time = time.time()
    latency_ms = (end_time - start_time) * 1000
    
    # Log API latency (Req 3)
    logger.info(json.dumps({
        "event": "API_Query_Latency",
        "endpoint": "/GetMetric",
        "latency_ms": f"{latency_ms:.3f}",
        "switch_id": switch_id
    }))

    return {"switch_id": switch_id, "metric_id": metric_id, "value": metric_value}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8080)
