import uvicorn
import asyncio
import logging
import time
import httpx
import pandas as pd
from io import StringIO
from fastapi import FastAPI, HTTPException, BackgroundTasks

# --- Configuration ---
INGESTION_INTERVAL_SECONDS = 10
TELEMETRY_SOURCE_URL = "http://127.0.0.1:9001/counters"
LOG_FILE = "aggregator_server.log"

# --- Logging Setup (Bonus Requirement) ---
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(LOG_FILE, mode='w'),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

# --- 1. The RCU Data Store ---
class TelemetryStore:
    """
    Manages the telemetry data using the Read-Copy-Update (RCU) pattern.
    Reads (API lookups) are non-blocking. Writes (ingestion) are protected only
    during the instantaneous atomic swap operation.
    """
    def __init__(self):
        # The public, exposed data copy (Readers access this without a lock)
        self.current_data = {} 
        # Lock to protect the single, atomic reference swap
        self.lock = asyncio.Lock()
        
    def get_data(self):
        """Returns the current snapshot for non-blocking reads."""
        return self.current_data

    async def atomic_swap(self, new_snapshot: dict):
        """
        Performs the atomic update (the RCU swap). 
        The lock minimizes the time readers are blocked to microseconds.
        """
        async with self.lock:
            # Atomic reference assignment to the new data snapshot
            self.current_data = new_snapshot
            logger.info(f"RCU Swap: Successfully updated store with {len(new_snapshot)} switch metrics.")

# Create a single instance of the Store
store = TelemetryStore()

# --- 2. Ingestion Utilities and Task ---

def parse_csv_to_dict(csv_content: str) -> dict:
    """
    Parses the CSV matrix from the generator into the required nested dictionary format.
    Uses Pandas for robust, high-performance CSV parsing.
    """
    if not csv_content:
        return {}
        
    df = pd.read_csv(StringIO(csv_content))
    
    # Set 'switch_id' as the index
    df = df.set_index('switch_id')
    
    # Convert DataFrame back to a nested dictionary for fast Python lookup
    # {'SW-00': {'Bandwidth_Rx_Gbps': 45.2, ...}, ...}
    return df.to_dict('index')

async def ingestion_task():
    """
    Background task to fetch data from the telemetry source and update the RCU store.
    """
    logger.info(f"Ingestion Task started. Polling URL: {TELEMETRY_SOURCE_URL}")
    
    # Use an async HTTP client for non-blocking I/O
    async with httpx.AsyncClient(timeout=INGESTION_INTERVAL_SECONDS) as client:
        while True:
            start_time = time.time()
            try:
                # --- FETCH DATA (I/O, outside of lock) ---
                response = await client.get(TELEMETRY_SOURCE_URL)
                response.raise_for_status() # Raise exception for 4xx/5xx status codes
                csv_content = response.text

                # --- PARSE DATA (CPU, outside of lock) ---
                new_data = parse_csv_to_dict(csv_content)
                
                if not new_data:
                    logger.warning("Ingestion: Received empty data snapshot.")
                else:
                    # --- ATOMIC SWAP (inside lock) ---
                    await store.atomic_swap(new_data)
                    
            except httpx.RequestError as e:
                logger.error(f"Ingestion failed (HTTP Request Error): Cannot connect to generator on {TELEMETRY_SOURCE_URL}. Error: {e}")
            except Exception as e:
                logger.error(f"Ingestion failed (Parsing/General Error): {e}")
            
            # --- Performance/Observability Logging ---
            end_time = time.time()
            latency = (end_time - start_time) * 1000
            logger.info(f"Ingestion cycle completed. Time taken: {latency:.2f}ms.")
            
            # Wait for the next cycle
            await asyncio.sleep(INGESTION_INTERVAL_SECONDS)

# --- 3. FastAPI Application ---
app = FastAPI(title="Network Telemetry Aggregator (Metrics Server)", 
              description="High-performance, non-blocking telemetry API using RCU.")

@app.on_event("startup")
async def startup_event():
    # Start the background data ingestion task
    asyncio.create_task(ingestion_task())
    logger.info("Metrics Server is running and Ingestion Task has been initiated.")

@app.get("/telemetry/ListMetrics", 
         summary="Fetch all current metrics for all switches.",
         response_model=dict)
async def list_metrics():
    """
    Fetches a list of all metrics and their values for every switch in the fabric.
    This is a highly performant, non-blocking read operation.
    """
    # The reader accesses the public copy directly, without a lock (RCU pattern)
    data = store.get_data()
    
    if not data:
        raise HTTPException(status_code=503, detail="Data unavailable. Store is empty or ingestion is not yet complete.")
    
    return data

@app.get("/telemetry/GetMetric", 
         summary="Fetch the current value of a specific metric for a specific switch.",
         response_model=dict)
async def get_metric(switch_id: str, metric_name: str):
    """
    Fetches the current value of a single metric for a specified switch.
    """
    # The reader accesses the public copy directly (RCU pattern)
    all_data = store.get_data()

    # 1. Check if store is populated
    if not all_data:
        raise HTTPException(status_code=503, detail="Data unavailable. Store is empty.")
    
    # 2. Check if switch exists
    switch_data = all_data.get(switch_id.upper())
    if not switch_data:
        raise HTTPException(status_code=404, detail=f"Switch ID '{switch_id.upper()}' not found.")
        
    # 3. Check if metric exists
    metric_value = switch_data.get(metric_name)
    if metric_value is None:
        valid_metrics = list(switch_data.keys())
        raise HTTPException(status_code=404, detail=f"Metric '{metric_name}' not found for switch '{switch_id.upper()}'. Valid metrics are: {', '.join(valid_metrics)}")

    return {
        "switch_id": switch_id.upper(),
        "metric_name": metric_name,
        "value": metric_value
    }

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
# --- End of aggregator_server.py ---