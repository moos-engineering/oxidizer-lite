"""Universal Reagent worker: passes each node's first input batch through to its outputs."""

import argparse
from oxidizer import Catalyst, Reagent, Residue
from .utils.helpers import create_catalyst_connection, convert_data_types, load_config, load_custom_router

# Parse Arguments
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--config", default="worker.yml", help="path to the worker YAML config (default: worker.yml)")
parser.add_argument("--worker", default="worker", help="name of worker object in YAML config (default: worker)")
args = parser.parse_args()

# Load Configuration
config = load_config(args.config)
catalyst_config = config.get("catalyst", {})
worker_config = config.get(args.worker, {})
custom_router = load_custom_router(
    worker_config.get("custom_functions_file"),
    worker_config.get("custom_functions_router"),
)

# Worker Details 
task_stream = worker_config.get("task_stream")
log_level = worker_config.get("log_level", "INFO")

# Initialize Residue, Catalyst, and Reagent
residue = Residue(log_level=log_level)
catalyst_connection = create_catalyst_connection(
    catalyst_config, residue, group_id=worker_config.get("kafka_group_id")
)
catalyst = Catalyst(catalyst_connection, residue=residue)
reagent = Reagent(catalyst, residue=residue)


# Define Reagent Reaction
@reagent.react(dedicated_stream=task_stream)
def process(data: dict, context: dict):
    residue.emit(
        residue.ash.DEBUG,
        "WORKER: Processing node",
        **context,
        input_sizes={name: len(value) if isinstance(value, list) else 1 for name, value in data.items()},
    )

    input_batches = list(data.values())
    output = input_batches[0] if input_batches else []
    return custom_router(output, context) if custom_router else output
