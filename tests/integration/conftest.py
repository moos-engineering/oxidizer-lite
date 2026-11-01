from pathlib import Path

import pytest
import yaml


def require_docker():
    docker = pytest.importorskip("docker")
    docker_client = None
    try:
        docker_client = docker.from_env()
        docker_client.ping()
    except Exception as exc:
        pytest.skip(f"Docker is not available for testcontainers: {exc}")
    finally:
        if docker_client is not None:
            docker_client.close()


@pytest.fixture(autouse=True)
def require_docker_for_integration(request):
    if request.node.get_closest_marker("external_services"):
        config_path = Path(__file__).with_name("recovery-boundaries.yml")
        config = yaml.safe_load(config_path.read_text())
        if config.get("mode") == "external":
            return
    require_docker()
