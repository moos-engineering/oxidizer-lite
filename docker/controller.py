"""Run the continuous Oxidizer controller using a YAML configuration."""

import argparse

from oxidizer import Catalyst, Formula, Oxidizer, Residue
from .utils.helpers import create_catalyst_connection, create_formula_connection, load_config

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--config", default="controller+worker.yml", help="Path to the controller YAML config")
args = parser.parse_args()

config = load_config(args.config)
formula_config = config.get("formula", {})
catalyst_config = config.get("catalyst", {})
controller_config = config.get("controller", {})

residue = Residue(log_level=controller_config.get("log_level", "INFO")).bind(lattice_id="oxidizer")
catalyst_connection = create_catalyst_connection(
	catalyst_config, residue, group_id=controller_config.get("kafka_group_id")
)
formula_connection = create_formula_connection(formula_config, residue)

formula = Formula(formula_connection, residue=residue)
catalyst = Catalyst(catalyst_connection, residue=residue)
oxidizer = Oxidizer(formula, catalyst, residue=residue, retention=controller_config.get("retention"))
oxidizer.oxidize()
