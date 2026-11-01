from oxidizer import CheckpointMetadata


def test_checkpoint_metadata_to_dict():
    checkpoint = CheckpointMetadata(
        node_id="demo.extract.users",
        batch_index=2,
        records_processed=10,
        total_records_so_far=25,
        total_late_arrivals_ignored=7,
    )

    assert checkpoint.to_dict()["node_id"] == "demo.extract.users"
    assert checkpoint.to_dict()["batch_index"] == 2
    assert checkpoint.to_dict()["records_processed"] == 10
    assert checkpoint.to_dict()["total_records_so_far"] == 25
    assert checkpoint.to_dict()["total_late_arrivals_ignored"] == 7
    assert checkpoint.to_dict()["peak_preprocess_memory_bytes"] is None
    assert checkpoint.to_dict()["peak_function_memory_bytes"] is None
    assert checkpoint.to_dict()["peak_postprocess_memory_bytes"] is None
    assert "accumulated_preprocess_memory" not in checkpoint.to_dict()