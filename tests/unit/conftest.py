"""
Unit test fixtures.

Minimal setup - unit tests use mocks so no external dependencies.
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(scope="session")
def project_root():
    """Return the project root path."""
    return PROJECT_ROOT
