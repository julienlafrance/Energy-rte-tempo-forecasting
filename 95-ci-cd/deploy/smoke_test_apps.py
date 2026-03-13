#!/usr/bin/env python3
"""Smoke tests for API & Webapp services (DEV and PROD).

Reads endpoint definitions from deploy_apps_smoke_tests.yaml and
verifies that each service responds with HTTP 200 on its health
endpoint. Exits with code 1 if any *required* endpoint fails.

Usage:
    python3 smoke_test_apps.py --env dev
    python3 smoke_test_apps.py --env prod

These are **observability checks only** — the CI/CD pipeline does NOT
build or deploy the API / Webapp. They are deployed separately by
the infrastructure team.
"""

import argparse
import sys
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError

import yaml

# ── Paths ────────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parents[2]

sys.path.insert(0, str(REPO_ROOT / "95-ci-cd" / "config"))
from load_config import load_repo_structure

_CONF = load_repo_structure()

CONFIG_PATH = REPO_ROOT / _CONF["config_files"]["apps_smoke_tests"]

VALID_ENVS = ("dev", "prod")


# ── Config loading ───────────────────────────────────────────────────────────

def load_config(path: Path = CONFIG_PATH) -> dict:
    """Load and validate deploy_apps_smoke_tests.yaml."""
    if not path.exists():
        print(f"❌ Config file not found: {path}")
        sys.exit(1)
    with open(path) as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(f"❌ YAML parse error in {path}: {exc}")
            sys.exit(1)
    if not isinstance(data, dict):
        print(f"❌ Invalid config format in {path}")
        sys.exit(1)
    for env_name in VALID_ENVS:
        section = data.get(env_name)
        if not isinstance(section, dict):
            print(f"❌ Missing or invalid '{env_name}' section in {path}")
            sys.exit(1)
        if "endpoints" not in section or not isinstance(section["endpoints"], list):
            print(f"❌ Missing or invalid 'endpoints' in {env_name} section of {path}")
            sys.exit(1)
        for ep in section["endpoints"]:
            for key in ("name", "url", "health_path"):
                if key not in ep:
                    print(f"❌ Endpoint missing '{key}' in {env_name} section of {path}")
                    sys.exit(1)
    return data


# ── HTTP helper ──────────────────────────────────────────────────────────────

def http_health_check(url: str) -> int:
    """Return the HTTP status code for a GET request (stdlib only)."""
    req = Request(url)
    try:
        resp = urlopen(req, timeout=15)  # noqa: S310 — URL from trusted config
        return resp.status
    except URLError as exc:
        if hasattr(exc, "code"):
            return exc.code  # type: ignore[return-value]
        print(f"  ⚠️  Connection error: {exc.reason}")
        return 0


# ── Check ────────────────────────────────────────────────────────────────────

def check_endpoints(endpoints: list[dict]) -> int:
    """Check each endpoint and return the number of required failures."""
    print("\n── Checking app endpoints ─────────────────────────")
    failures = 0
    for ep in endpoints:
        name = ep["name"]
        url = ep["url"].rstrip("/") + ep["health_path"]
        required = ep.get("required", True)
        status = http_health_check(url)
        if status == 200:
            print(f"  ✅ {name} — {url} returned 200")
        elif required:
            print(f"  ❌ {name} — {url} returned HTTP {status}")
            failures += 1
        else:
            print(f"  ⚠️  {name} — {url} returned HTTP {status} (non-required)")
    return failures


# ── Main ─────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="App health smoke tests")
    parser.add_argument(
        "--env", choices=VALID_ENVS, required=True,
        help="Target environment (dev or prod)",
    )
    args = parser.parse_args(argv)

    config = load_config()
    env_config = config[args.env]

    print(f"App smoke tests — {args.env} endpoints")
    print(f"Config: {CONFIG_PATH}")

    failures = check_endpoints(env_config["endpoints"])

    print("\n────────────────────────────────────────────────────")
    if failures > 0:
        print(f"❌ {failures} required endpoint(s) failed")
        return 1
    print("✅ All app smoke tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
