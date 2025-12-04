import pytest
from fastapi.testclient import TestClient
from aggregator_server import app, telemetry_store

client = TestClient(app)

def test_api_list_metrics_initially_unavailable():
    # Before the store is ready, it should return 503
    telemetry_store.update_metrics("is_ready", False)
    response = client.get("/health")
    assert response.status_code == 503

def test_api_list_metrics_success():
    # Manually set the store to a ready state with some data
    test_data = {"SW-01": {"metric": "value"}}
    telemetry_store.current_data = test_data
    telemetry_store.update_metrics("is_ready", True)
    
    # Test the /telemetry/ListMetrics endpoint
    response = client.get("/telemetry/ListMetrics")
    assert response.status_code == 200
    assert response.json() == test_data

def test_api_get_specific_metric_success():
    # Manually set the store data
    test_data = {"SW-01": {"metric_A": "value_A"}}
    telemetry_store.current_data = test_data
    
    # Test successful metric retrieval
    response = client.get("/telemetry/GetMetric/SW-01/metric_A")
    assert response.status_code == 200
    assert response.json() == {
        "switch_id": "SW-01",
        "metric_id": "metric_A",
        "value": "value_A"
    }

def test_api_get_specific_metric_not_found():
    # Manually set the store data
    telemetry_store.current_data = {"SW-01": {"metric_A": "value_A"}}
    
    # Test for a metric that doesn't exist
    response = client.get("/telemetry/GetMetric/SW-01/metric_B")
    assert response.status_code == 404
