# generator.py
import uvicorn
import random
import csv
from io import StringIO
from fastapi import FastAPI, Response, HTTPException
from fastapi.responses import PlainTextResponse
import time
import sys

# --- Configuration ---
NUM_SWITCHES = 50
SWITCH_PREFIX = "SW-"

# --- State Variables for Simulation ---
telemetry_data = {} 
SIMULATION_MODE = "NORMAL" # Can be set via query parameter

# --- Data Generation Functions ---

def generate_telemetry_snapshot():
    """Generates a new, random set of metrics for all switches."""
    global telemetry_data
    
    new_snapshot = {}
    for i in range(NUM_SWITCHES):
        switch_id = f"{SWITCH_PREFIX}{i:02d}"
        
        # Simulate realistic random data
        bandwidth = round(random.uniform(0.0, 100.0), 1)
        latency = round(random.uniform(1.0, 500.0), 1)
        # Introduce a spike risk for Latency/Errors 5% of the time
        if random.random() < 0.05:
            errors = random.randint(500, 1000) # High error spike
            latency = round(random.uniform(1000.0, 5000.0), 1) # High latency spike
        else:
            errors = random.randint(0, 10)
        
        port_status = "DOWN" if random.random() < 0.02 else "UP" # 2% chance of a port down
        
        new_snapshot[switch_id] = {
            "Bandwidth_Rx_Gbps": bandwidth,
            "Latency_Avg_uSec": latency,
            "Error_CRC_Count": errors,
            "Port_Status": port_status
        }
    
    telemetry_data = new_snapshot
    print(f"[{time.strftime('%H:%M:%S')}] Generator: New snapshot generated for {NUM_SWITCHES} switches.")

def get_csv_matrix(corrupt=False):
    """Converts the current dictionary data into the required CSV matrix format."""
    
    if not telemetry_data:
        return ""

    metrics = list(telemetry_data[f"{SWITCH_PREFIX}00"].keys())
    fieldnames = ['switch_id'] + metrics

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    
    writer.writeheader()
    
    for switch_id, metrics in telemetry_data.items():
        row = {'switch_id': switch_id}
        row.update(metrics)
        writer.writerow(row)
        
    content = output.getvalue()
    
    if corrupt:
        # Simulate CSV corruption by cutting off the end
        return content[:-random.randint(50, 100)] 
    
    return content


# --- FastAPI App ---
app = FastAPI(title="Telemetry Data Server (Producer)")

@app.on_event("startup")
async def startup_event():
    # Initial data generation on startup
    generate_telemetry_snapshot()
    print("Telemetry Generator started on 127.0.0.1:9001. Use /counters?mode=ERROR to simulate failure.")


@app.get("/counters", summary="Returns all telemetry data in CSV matrix format")
async def get_counters(mode: str = "NORMAL"):
    """
    Implements the required GET http://127.0.0.1:9001/counters endpoint with simulation modes.
    Checks the URL query parameter first, then falls back to the sticky command-line mode.
    """
    global SIMULATION_MODE
    
    # Determine the effective mode: Use the query parameter if provided, otherwise use the global sticky mode
    effective_mode = mode.upper() if mode.upper() != "NORMAL" else SIMULATION_MODE

    if effective_mode == "ERROR_500":
        print(f"[{time.strftime('%H:%M:%S')}] Generator: Simulating HTTP 500 Internal Error.")
        # This is where the error is raised, preventing the rest of the function from running.
        raise HTTPException(status_code=500, detail="Simulated Internal Server Error")
    
    if effective_mode == "SLOW":
        print(f"[{time.strftime('%H:%M:%S')}] Generator: Simulating 5s network congestion delay.")
        time.sleep(5)
    
    corrupt = (effective_mode == "CORRUPT")
    
    # Re-generate data for fresh metrics on every call (simulating constant updates)
    generate_telemetry_snapshot()
    
    csv_content = get_csv_matrix(corrupt=corrupt)
    
    # Return as a plain text response with the correct Content-Type
    return PlainTextResponse(content=csv_content, media_type="text/csv")


if __name__ == "__main__":
    # If a mode is passed via command line, it's sticky
    if len(sys.argv) > 1 and sys.argv[1].upper() in ["ERROR_500", "CORRUPT", "SLOW"]:
        SIMULATION_MODE = sys.argv[1].upper()
        print(f"Generator running in permanent mode: {SIMULATION_MODE}")
    
    uvicorn.run(app, host="127.0.0.1", port=9001)

# --- End of generator.py ---
