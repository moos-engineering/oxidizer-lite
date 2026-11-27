"""Run the Topology API and MCP server using a YAML configuration."""

import argparse
from collections.abc import Mapping
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import uvicorn

from oxidizer import Catalyst, Formula, Residue
from oxidizer.utils.topology import Topology
from .utils.helpers import create_catalyst_connection, create_formula_connection, load_config

if TYPE_CHECKING:
	from fastmcp.server.auth import AuthProvider


def build_topology_auth(config: Mapping[str, object]) -> "AuthProvider | None":
	topology_config = config.get("topology", {})
	if not isinstance(topology_config, Mapping):
		raise ValueError("topology must be a mapping")
	if "oauth" not in topology_config:
		return None

	oauth = topology_config["oauth"]
	if not isinstance(oauth, Mapping):
		raise ValueError("topology.oauth must be a mapping")
	if oauth.get("provider") != "github":
		raise ValueError("topology.oauth.provider must be 'github'")
	if oauth.keys() - {"provider", "client_id", "client_secret", "base_url", "allowed_github_user_ids"}:
		raise ValueError("topology.oauth contains unsupported settings")
	client_id = oauth.get("client_id")
	client_secret = oauth.get("client_secret")
	base_url = oauth.get("base_url")
	if not isinstance(client_id, str) or not client_id.strip():
		raise ValueError("topology.oauth.client_id must be a nonempty string")
	if not isinstance(client_secret, str) or not client_secret.strip():
		raise ValueError("topology.oauth.client_secret must be a nonempty string")
	if not isinstance(base_url, str):
		raise ValueError("topology.oauth.base_url must be a public HTTPS origin")
	url = urlsplit(base_url)
	if (
		not url.hostname or url.username is not None or url.path not in ("", "/")
		or any(char.isspace() or char in "?#\\" for char in base_url) or url.port == 0
		or (url.scheme != "https" and not (
			url.scheme == "http" and url.hostname in ("localhost", "127.0.0.1", "::1")
		))
	):
		raise ValueError("topology.oauth.base_url must be a public HTTPS origin (HTTP is allowed for loopback)")
	user_ids = oauth.get("allowed_github_user_ids")
	if not isinstance(user_ids, list) or not user_ids or any(type(user_id) is not int or user_id < 1 for user_id in user_ids):
		raise ValueError("topology.oauth.allowed_github_user_ids must be a nonempty list of positive integers")
	allowed_subjects = frozenset(str(user_id) for user_id in user_ids)

	from fastmcp.server.auth import AccessToken
	from fastmcp.server.auth.providers.github import GitHubProvider

	class AllowedGitHubProvider(GitHubProvider):
		async def verify_token(self, token: str) -> AccessToken | None:
			verified = await super().verify_token(token)
			return verified if verified is not None and verified.subject in allowed_subjects else None

	origin = base_url.rstrip("/")
	# OAuth handlers live inside the mount, while discovery lives at the origin.
	return AllowedGitHubProvider(
		client_id=client_id, client_secret=client_secret, base_url=f"{origin}{Topology.MCP_MOUNT}",
		issuer_url=origin, required_scopes=[],
	)


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--config", default="controller+worker.yml", help="path to the API YAML config")
parser.add_argument("--host", default="0.0.0.0", help="HTTP bind address")
parser.add_argument("--port", type=int, default=8000, help="HTTP bind port")
parser.add_argument("--log-level", default="info", help="logging level")
args = parser.parse_args()

config = load_config(args.config)
auth = build_topology_auth(config)
formula_config = config.get("formula", {})
catalyst_config = config.get("catalyst", {})

residue = Residue(log_level=args.log_level.upper()).bind(lattice_id="oxidizer")
catalyst_connection = create_catalyst_connection(catalyst_config, residue)
formula_connection = create_formula_connection(formula_config, residue)

formula = Formula(formula_connection, residue=residue)
catalyst = Catalyst(catalyst_connection, residue=residue)

topology = Topology(formula, catalyst, auth=auth)

topology.register_api_endpoints()
topology.register_mcp_tools()

uvicorn.run(
	topology.app,
	host=args.host,
	port=args.port,
	log_level=args.log_level.lower(),
)
