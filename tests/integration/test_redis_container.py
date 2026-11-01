import time

import pytest

pytestmark = pytest.mark.integration


def test_redis_container_smoke():
    pytest.importorskip("redis")
    testcontainers_redis = pytest.importorskip("testcontainers.community.redis")

    from oxidizer import Catalyst, RedisConnection

    with testcontainers_redis.RedisContainer("redis:7-alpine") as container:
        connection_url = f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}/0"
        connection = RedisConnection(url=connection_url)
        catalyst = Catalyst(connection)

        catalyst.create_stream("oxidizer-integration")
        message_id = catalyst.write_to_stream("oxidizer-integration", {"ok": True})

        assert message_id


def test_redis_reconnects_and_retries_pending_message_after_restart():
    redis = pytest.importorskip("redis")
    testcontainers_redis = pytest.importorskip("testcontainers.community.redis")

    from oxidizer import Catalyst, RedisConnection

    with testcontainers_redis.RedisContainer("redis:7-alpine").with_command(
        "redis-server --appendonly yes --appendfsync always"
    ) as container:
        connection_url = (
            f"redis://{container.get_container_host_ip()}"
            f":{container.get_exposed_port(6379)}/0"
        )
        connection = RedisConnection(
            url=connection_url,
            socket_timeout=1,
            socket_connect_timeout=1,
        )
        catalyst = Catalyst(connection)
        stream_name = "oxidizer-recovery"
        group_name = "recovery-workers"
        owner = "worker-1"
        catalyst.create_consumer_group(stream_name, group_name)
        message_id = catalyst.write_to_stream(stream_name, {"id": 1})

        first_delivery = catalyst.read_from_stream(
            stream_name, group_name, owner, count=1, block=1000
        )
        assert first_delivery == [(message_id, {"message": {"id": 1}})]
        assert connection.client.xpending(stream_name, group_name)["pending"] == 1

        container.stop()
        with pytest.raises(redis.exceptions.ConnectionError):
            connection.client.ping()

        container.start()
        deadline = time.monotonic() + 30
        while True:
            try:
                assert connection.client.ping()
                break
            except redis.exceptions.ConnectionError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.25)

        assert connection.client.xpending(stream_name, group_name)["pending"] == 1
        retry_delivery = catalyst.read_from_stream(
            stream_name, group_name, owner, count=1, block=1000, stream_id="0"
        )
        assert retry_delivery == first_delivery

        other_consumer_delivery = catalyst.read_from_stream(
            stream_name, group_name, "worker-2", count=1, block=50, stream_id="0"
        )
        assert other_consumer_delivery == []

        assert catalyst.ack_messages(
            stream_name, group_name, [message_id], consumer_name=owner
        ) == 1
        assert connection.client.xpending(stream_name, group_name)["pending"] == 0
        assert catalyst.read_from_stream(
            stream_name, group_name, owner, count=1, block=50, stream_id="0"
        ) == []


def test_redis_json_formula_container_smoke():
    pytest.importorskip("redis")
    testcontainers_redis = pytest.importorskip("testcontainers.community.redis")

    from oxidizer import Formula, RedisConnection

    with testcontainers_redis.RedisContainer("redis/redis-stack-server:latest") as container:
        connection_url = f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}/0"
        connection = RedisConnection(url=connection_url)
        formula = Formula(connection)

        formula.create_formula_config("redis-json-demo", {"layers": []})
        formula.create_formula_state("redis-json-demo", {"status": "created"}, run_id="run-123")

        assert formula.load_formula_config("redis-json-demo") == {"layers": []}
        assert formula.load_formula_state("redis-json-demo", run_id="run-123") == {"status": "created"}
        assert "redis-json-demo" in formula.list_formula_configs()
        assert "redis-json-demo:run-123" in formula.list_formula_states()

        formula.update_formula_state("redis-json-demo", {"status": "updated"}, run_id="run-123")
        assert formula.load_formula_state("redis-json-demo", run_id="run-123") == {"status": "updated"}

        formula.update_formula_state(
            "redis-json-demo",
            {"status": "patched", "checkpoint": {"records_processed": 10}},
            run_id="run-123",
            path="$.checkpoint",
        )
        assert formula.load_formula_state("redis-json-demo", run_id="run-123") == {
            "status": "updated",
            "checkpoint": {"records_processed": 10},
        }

        formula.expire_formula_state("redis-json-demo", run_id="run-123")
        assert "redis-json-demo:run-123" not in formula.list_formula_states()