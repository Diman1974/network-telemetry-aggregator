import asyncio
import logging
from functools import wraps
from io import StringIO
import csv
from typing import Dict, Any, Callable

LOG_FILE = "aggregator_server.log"

logger = logging.getLogger("aggregator")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(LOG_FILE, mode='a')
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

def parse_csv_data(csv_content: str) -> Dict[str, Dict[str, Any]]:
    """Parses telemetry CSV into a nested dict keyed by switch_id."""
    if not csv_content.strip():
        return {}

    normalized = (
        csv_content
        .replace("\\r\\n", "\n")
        .replace("\\n", "\n")
        .replace("\\r", "")
    )

    reader = csv.reader(StringIO(normalized))
    try:
        header = next(reader)
    except StopIteration:
        return {}

    if 'switch_id' not in header:
        raise ValueError("CSV missing required 'switch_id' column.")

    switch_idx = header.index('switch_id')
    result: Dict[str, Dict[str, Any]] = {}

    for row_number, row in enumerate(reader, start=1):
        if len(row) != len(header):
            raise ValueError("Row has an incorrect number of fields.")

        switch_id = row[switch_idx].strip()
        metrics: Dict[str, Any] = {}
        for idx, column_name in enumerate(header):
            if idx == switch_idx:
                continue
            metrics[column_name] = row[idx]

        result[switch_id] = metrics

    return result

def retry_with_backoff(func=None, *, retries: int = 3, backoff_in_seconds: float = 1.0):
    """Decorator adding exponential backoff retry logic to async functions."""
    def decorator(async_func: Callable):
        @wraps(async_func)
        async def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return await async_func(*args, **kwargs)
                except Exception as exc:
                    attempt += 1
                    if attempt > retries:
                        logger.error("Max retries exceeded for %s: %s", async_func.__name__, exc)
                        raise

                    delay = backoff_in_seconds * (2 ** (attempt - 1))
                    logger.warning("Retrying %s in %.2fs (attempt %d/%d)",
                                   async_func.__name__, delay, attempt, retries)
                    await asyncio.sleep(delay)
        return wrapper

    if callable(func):
        return decorator(func)
    return decorator