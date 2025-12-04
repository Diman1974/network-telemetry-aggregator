import pytest
import asyncio
from aggregator_server import AggregatorStore

@pytest.mark.asyncio
async def test_rcu_swap_and_get():
    store = AggregatorStore()
    
    # 1. Initially, data should be empty
    assert store.get_data() == {}
    
    # 2. Create a new data snapshot
    snapshot_1 = {"SW-01": {"metric": 100}}
    
    # 3. Perform the atomic swap
    await store._swap_data(snapshot_1)
    
    # 4. The reader should now see the new data
    assert store.get_data() == snapshot_1
    
    # 5. Create a second snapshot
    snapshot_2 = {"SW-02": {"metric": 200}}
    
    # 6. Perform another swap
    await store._swap_data(snapshot_2)
    
    # 7. The reader should see the second snapshot, not the first
    assert store.get_data() == snapshot_2
    # Ensure the original snapshot object is not modified
    assert snapshot_1 == {"SW-01": {"metric": 100}}
