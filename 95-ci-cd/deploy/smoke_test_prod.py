#!/usr/bin/env python3
"""Smoke tests for Kestra deployments (DEV and PROD).

Reads the expected state from deploy_smoke_tests.yaml and verifies
it against the live Kestra API. Exits with code 1 on any failure.

Required environment variables:
    KESTRA_SERVER     Base URL of the Kestra server
    KESTRA_NAMESPACE  Target namespace
    KESTRA_USER       API username
    KESTRA_PASS       API password
    SQL_DIR           Path to the SQL directory (e.g. 140-sql)
"""

import glob
import os
import sys
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError
import base64
import yaml

# ── Paths ────────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parents[2]

# Import central config loader
sys.path.insert(0, str(REPO_ROOT / "95-ci-cd" / "config"))
from load_config import load_repo_structure

_CONF = load_repo_structure()

CONFIG_PATH = REPO_ROOT / _CONF["config_files"]["smoke_tests"]
DEFAULT_SQL_DIR = _CONF["directories"]["sql"]


# ── Config loading ───────────────────────────────────────────────────────────

def load_config(path: Path = CONFIG_PATH) -> dict:
    """Load and validate deploy_smoke_tests.yaml."""
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
    for key in ("expected_flows", "critical_kv_keys"):
        if key not in data or not isinstance(data[key], list):
            print(f"❌ Missing or invalid '{key}' in {path}")
            sys.exit(1)
    return data


# ── HTTP helper ──────────────────────────────────────────────────────────────

def _http_status(url: str, user: str, password: str) -> int:
    """Return the HTTP status code for a GET request (stdlib only)."""
    credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
    req = Request(url)
    req.add_header("Authorization", f"Basic {credentials}")
    try:
        resp = urlopen(req, timeout=10)  # noqa: S310 — URL built from trusted env vars
        return resp.status
    except URLError as exc:
        if hasattr(exc, "code"):
            return exc.code  # type: ignore[return-value]
        print(f"  ⚠️  Connection error: {exc.reason}")
        return 0


# ── Checks ───────────────────────────────────────────────────────────────────

def check_flows(server: str, namespace: str, user: str, password: str,
                expected: list[str]) -> int:
    """Verify that each expected flow exists via the Kestra API."""
    print("\n── Checking expected flows ─────────────────────────")
    failures = 0
    for flow_id in expected:
        url = f"{server}/api/v1/main/flows/{namespace}/{flow_id}"
        status = _http_status(url, user, password)
        if status != 200:
            print(f"  ❌ Flow {flow_id} not found (HTTP {status})")
            failures += 1
        else:
            print(f"  ✅ {flow_id}")
    return failures


def check_namespace_files(server: str, namespace: str, user: str,
                          password: str, sql_dir: str) -> int:
    """Verify that every SQL file tracked in Git exists as a namespace file."""
    print("\n── Checking namespace files (SQL) ──────────────────")
    queries_dir = os.path.join(sql_dir, "queries")
    sql_files = sorted(glob.glob(os.path.join(queries_dir, "*.sql")))
    if not sql_files:
        print("  ⚠️  No SQL files found — skipping")
        return 0
    failures = 0
    for sql_file in sql_files:
        rel_path = f"queries/{os.path.basename(sql_file)}"
        url = f"{server}/api/v1/main/namespaces/{namespace}/files?path={rel_path}"
        status = _http_status(url, user, password)
        if status != 200:
            print(f"  ❌ Namespace file {rel_path} not found (HTTP {status})")
            failures += 1
        else:
            print(f"  ✅ {rel_path}")
    return failures


def check_kv_keys(server: str, namespace: str, user: str, password: str,
                  keys: list[str], *, strict: bool = False) -> int:
    """Verify that each critical KV key is accessible.

    When strict=False (default), missing keys are warnings.
    When strict=True, missing keys are counted as failures.
    """
    print("\n── Checking critical KV keys ───────────────────────")
    if strict:
        print("   (strict mode — missing keys are errors)")
    failures = 0
    for key in keys:
        url = f"{server}/api/v1/main/namespaces/{namespace}/kv/{key}"
        status = _http_status(url, user, password)
        if status != 200:
            if strict:
                print(f"  ❌ KV key {key} not accessible (HTTP {status})")
                failures += 1
            else:
                print(f"  ⚠️  KV key {key} not accessible (HTTP {status})")
        else:
            print(f"  ✅ KV {key}")
    return failures


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    server = os.environ.get("KESTRA_SERVER", "")
    namespace = os.environ.get("KESTRA_NAMESPACE", "")
    user = os.environ.get("KESTRA_USER", "")
    password = os.environ.get("KESTRA_PASS", "")
    sql_dir = os.environ.get("SQL_DIR", DEFAULT_SQL_DIR)

    if not all([server, namespace, user, password]):
        print("❌ Missing required env vars: KESTRA_SERVER, KESTRA_NAMESPACE, KESTRA_USER, KESTRA_PASS")
        return 1

    config = load_config()

    print(f"Smoke tests — {server} / namespace: {namespace}")
    print(f"Config: {CONFIG_PATH}")

    total_failures = 0
    total_failures += check_flows(server, namespace, user, password,
                                  config["expected_flows"])
    total_failures += check_namespace_files(server, namespace, user,
                                            password, sql_dir)
    total_failures += check_kv_keys(server, namespace, user, password,
                                    config["critical_kv_keys"])

    print("\n────────────────────────────────────────────────────")
    if total_failures > 0:
        print(f"❌ {total_failures} check(s) failed")
        return 1
    print("✅ All smoke tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
