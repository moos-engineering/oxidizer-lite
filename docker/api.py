"""Run the Topology API and MCP server using a YAML configuration."""

import argparse

import uvicorn

from oxidizer import Catalyst, Formula, Residue
from oxidizer.utils.topology import Topology
from .utils.helpers import create_catalyst_connection, create_formula_connection, load_config


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--config", default="controller+worker.yml", help="path to the API YAML config")
parser.add_argument("--host", default="0.0.0.0", help="HTTP bind address")
parser.add_argument("--port", type=int, default=8000, help="HTTP bind port")
parser.add_argument("--log-level", default="info", help="logging level")
args = parser.parse_args()

config = load_config(args.config)
formula_config = config.get("formula", {})
catalyst_config = config.get("catalyst", {})

residue = Residue(log_level=args.log_level.upper()).bind(lattice_id="oxidizer")
catalyst_connection = create_catalyst_connection(catalyst_config, residue)
formula_connection = create_formula_connection(formula_config, residue)

formula = Formula(formula_connection, residue=residue)
catalyst = Catalyst(catalyst_connection, residue=residue)

topology = Topology(formula, catalyst)

topology.register_api_endpoints()
topology.register_mcp_tools()

uvicorn.run(
	topology.app,
	host=args.host,
	port=args.port,
	log_level=args.log_level.lower(),
)
