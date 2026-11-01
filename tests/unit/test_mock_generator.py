from datetime import datetime, timezone

import pytest

mock_custom = pytest.importorskip("examples.mock.custom", reason="Optional Mock example is not included in this checkout")
TEMPLATE = mock_custom.TEMPLATE
generate_mock_records = mock_custom.generate_mock_records


def test_generate_events_uses_top_level_settings(monkeypatch):
	monkeypatch.setattr(mock_custom, "MOCK_SIZE", 3)
	monkeypatch.setattr(mock_custom, "MOCK_UNIT", "hours")
	monkeypatch.setattr(mock_custom, "MOCK_ENTITIES", 2)
	monkeypatch.setattr(mock_custom, "MOCK_SEED", 42)
	monkeypatch.setattr(mock_custom, "MOCK_END_AT", "2026-10-13T12:00:00Z")

	rows = mock_custom.generate_events(None, {})

	assert len(rows) == 6
	assert rows[-1]["observed_at"] == "2026-10-13T12:00:00.000000Z"


def test_custom_source_generates_template_fields_in_time_order():
	rows = generate_mock_records(
		TEMPLATE,
		size=3,
		unit="hours",
		entities=2,
		seed=42,
		end_at=datetime(2026, 10, 13, 12, tzinfo=timezone.utc),
	)

	assert len(rows) == 6
	assert set(rows[0]) == set(TEMPLATE)
	assert [row["observed_at"] for row in rows] == sorted(row["observed_at"] for row in rows)
	assert {row["entity_id"] for row in rows} == {"entity-000000", "entity-000001"}
	assert len({row["observed_at"] for row in rows}) == 3


def test_custom_source_is_repeatable_and_revision_changes_attributes():
	options = {
		"size": 3,
		"unit": "hours",
		"entities": 2,
		"seed": 42,
		"end_at": datetime(2026, 10, 13, 12, tzinfo=timezone.utc),
	}
	first = generate_mock_records(TEMPLATE, **options)
	replay = generate_mock_records(TEMPLATE, **options)
	revision = generate_mock_records(TEMPLATE, **options, revision=1)

	assert first == replay
	assert first != revision
	assert [row["observed_at"] for row in first] == [row["observed_at"] for row in revision]


def test_custom_source_rejects_unsafe_record_volume():
	with pytest.raises(ValueError, match="mock record count"):
		generate_mock_records(TEMPLATE, size=10_000_000, unit="days", entities=2, seed=1)


def test_mock_sql_readback_node_depends_on_history_and_routes_records():
	from pathlib import Path
	import yaml
	from oxidizer import Formula, S3Connection

	document = yaml.safe_load((Path(__file__).parents[2] / "examples/mock/workflows/workflow.yml").read_text())
	formula = Formula(S3Connection(bucket="configs", client=object()))
	dag = formula.dag(document["name"], document["workflow"])
	node_id = "mock_event_history.export.read_history"
	node = dag["nodes"][node_id]
	method = node["inputs"][0]["methods"][0]
	assert dag["reverse_edges"][node_id] == ["mock_event_history.history.track_history"]
	assert method["method"] == "sql"
	assert method["catalogs"] == [{"connection": "local_lake", "catalog": "lake"}]
	assert method["batch_size"] == 3
	assert "WHERE is_current" in method["query"]
	assert "ORDER BY entity_id" in method["query"]
	assert node["outputs"]["methods"] == [{"method": "stream", "stream": "mock_event_history.current_events"}]
	records = [{"entity_id": "entity-000000", "observed_at": "2026-10-17 00:00:00+00"}]
	assert mock_custom.sample_router(records, {"node_name": "read_history"}) is records


def test_mock_triggered_sql_nodes_chain_entity_parameters():
	from pathlib import Path
	import yaml
	from oxidizer import Formula, S3Connection

	document = yaml.safe_load((Path(__file__).parents[2] / "examples/mock/workflows/workflow.yml").read_text())
	dag = Formula(S3Connection(bucket="configs", client=object())).dag(document["name"], document["workflow"])
	list_id = "mock_event_history.export.list_entities"
	detail_id = "mock_event_history.detail.entity_history"
	assert dag["reverse_edges"][list_id] == ["mock_event_history.history.track_history"]
	assert dag["reverse_edges"][detail_id] == [list_id]
	method = dag["nodes"][detail_id]["inputs"][0]["methods"][0]
	assert method["input_trigger"] == {"type": "stream", "attribute": "parameters"}
	assert "WHERE entity_id = ?" in method["query"] and "parameters" not in method
	assert mock_custom.sample_router([{"entity_id": "entity-000003"}], {"node_name": "list_entities"}) == [
		{"parameters": ["entity-000003"]}
	]