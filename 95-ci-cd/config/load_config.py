"""Shared config loader for repo_structure.yaml.

Provides a single function that Python scripts use to read the
central configuration instead of hardcoding paths and constants.
"""

from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_PATH = _REPO_ROOT / "95-ci-cd" / "config" / "repo_structure.yaml"


def load_repo_structure(config_path: Path | None = None) -> dict:
    """Load and return the repo_structure.yaml configuration.

    Parameters
    ----------
    config_path : Path, optional
        Override for tests. Defaults to the versioned config file.

    Returns the parsed dict (directories, kestra, scripts_policy, …).
    """
    path = config_path or _CONFIG_PATH
    with open(path) as f:
        return yaml.safe_load(f)
