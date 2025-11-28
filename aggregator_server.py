# aggregator_server.py
import uvicorn
import asyncio
import time
import pandas as pd
import httpx
import logging
from fastapi import FastAPI, HTTPException
from io import StringIO
# --- Configuration ---
TELEMETRY_SOURCE_URL = "http://127.0.0.1:9001/counters"
INGESTION_INTERVAL_SECONDS = 10 
# Max time to wait for the generator to respond
HTTP_TIMEOUT_SECONDS = 8 

# --- Logging Setup ---
# This ensures logs go to both the file and the console for easier debugging and meets observability requirement.
log_file_path = 'aggregator_server.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),  # Handler for the file
        logging.StreamHandler()              # Handler for the console (Terminal 2)
    ]
)

# --- Core RCU Data Store Implementation ---

class AggregatorServer:
    """
    Implements the Read-Copy-Update (RCU) pattern for non-blocking reads.
    The background task is the Writer; API endpoints are the Readers.
    """
    def __init__(self):
        # The public data store (The 'Read' copy)
        self.current_data = {} 
        # Lock protects only the instantaneous pointer swap, not the data access
        self.swap_lock = asyncio.Lock() 

    def get_data(self, switch_id: str = None, metric_name: str = None):
        """
        Public method for non-blocking data access (The Read).
        """
        data = self.current_data
        
        if not data:
            raise HTTPException(status_code=503, detail="Data unavailable. Store is empty; waiting for initial ingestion.")
        
        if switch_id is None:
            # ListMetrics request
            return data
        
        # GetMetric request
        if switch_id not in data:
            raise HTTPException(status_code=404, detail=f"Switch ID '{switch_id}' not found.")
        
        if metric_name is None:
            return data[switch_id]

        if metric_name not in data[switch_id]:
            valid_metrics = list(data[switch_id].keys())
            raise HTTPException(status_code=404, detail=f"Metric '{metric_name}' not found for switch '{switch_id}'. Valid metrics are: {', '.join(valid_metrics)}")
        
        return {metric_name: data[switch_id][metric_name]}

    def _parse_csv_to_dict(self, csv_content: str) -> dict:
        """Parses the CSV content into the required nested dictionary format."""
        
        # Use pandas for robust, efficient CSV parsing
        df = pd.read_csv(StringIO(csv_content))
        df = df.set_index('switch_id')
        
        # Convert DataFrame to the required nested dictionary structure
        return df.to_dict('index')

    async def _swap_data(self, new_snapshot: dict):
        """Performs the atomic RCU pointer swap."""
        async with self.swap_lock:
            # The instantaneous swap is the only part requiring a lock
            self.current_data = new_snapshot 

    async def ingestion_task(self):
        """
        Asynchronous background task to periodically fetch and update data (RCU Writer).
        This loop is designed to be resilient and non-crashing.
        """
        # Client initialized here so it lives for the lifetime of the task
        client = httpx.AsyncClient() 
        
        logging.info("Background ingestion task started.")

        while True:
            start_time = time.time()
            
            try:
                # 1. Fetch data from the generator
                response = await client.get(TELEMETRY_SOURCE_URL, timeout=HTTP_TIMEOUT_SECONDS)
                # raise_for_status() is key for error handling (raises exception for 4xx/5xx)
                response.raise_for_status() 

                # 2. Parse data and perform RCU update
                # Parsing is done *outside* the lock
                new_data = self._parse_csv_to_dict(response.text)
                
                # Atomic RCU swap (protected by lock, but very fast)
                await self._swap_data(new_data)
                
                end_time = time.time()
                # Log performance stat
                logging.info(f"RCU Swap: Successfully updated store. Time taken: {(end_time - start_time) * 1000:.0f}ms")

            except httpx.RequestError as e:
                # Catches network errors (connection refused, DNS) and HTTP status errors (500, 404)
                logging.error(f"Ingestion failed (HTTP Request/Network Error): {e}. Retaining old data.")
            except Exception as e:
                # Catches all other errors, primarily parsing failures (e.g., CORRUPT mode)
                logging.error(f"Ingestion failed (Parsing/General Error): {e}. Retaining old data.")

            # 3. Pause for the next cycle (crucial for concurrency)
            await asyncio.sleep(INGESTION_INTERVAL_SECONDS)

# --- FastAPI App Setup ---
app = FastAPI(title="Metrics Aggregation Server")

# Instantiate the data store globally
telemetry_store = AggregatorServer() 

@app.on_event("startup")
async def startup_event():
    """Starts the background ingestion task when the server starts."""
    logging.info("Application startup initiated. Starting ingestion task...")
    
    # CORRECT FIX: Call the method on the instantiated object (telemetry_store)
    # This prevents the "missing 'self'" error
    asyncio.create_task(telemetry_store.ingestion_task()) 


# --- API Endpoints (Readers) ---

# In aggregator_server.py

@app.get("/telemetry/ListMetrics", 
         summary="Fetch all current metrics for all switches.",
         response_model=dict)
async def list_metrics():
    """Retrieves the full telemetry snapshot from the RCU store."""
    
    start_time = time.time()  # Start timer
    
    # 1. Get data (fast, non-blocking RCU read)
    response_data = telemetry_store.get_data()
    
    end_time = time.time()
    latency_ms = (end_time - start_time) * 1000
    
    # 2. Log API latency (New Requirement)
    logging.info(f"API Latency: ListMetrics completed in {latency_ms:.2f}ms.")
    
    return response_data

@app.get("/telemetry/GetMetric", 
         summary="Fetch a specific metric value for a given switch ID.",
         response_model=dict)
async def get_metric(switch_id: str, metric_name: str = None):
    """Retrieves a specific metric or all metrics for one switch."""
    if metric_name:
        return telemetry_store.get_data(switch_id=switch_id, metric_name=metric_name)
    else:
        return telemetry_store.get_data(switch_id=switch_id)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
