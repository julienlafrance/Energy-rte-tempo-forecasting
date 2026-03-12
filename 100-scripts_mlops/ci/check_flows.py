#!/usr/bin/env python3
"""
Kestra flow validator for CI.

Checks:
  - YAML syntax
  - Required top-level fields (id, namespace, tasks)
  - No duplicate flow IDs across the repository
  - Subflow references resolve to known flow IDs
  - No hardcoded credentials (passwords, secrets in plain text)
  - Consistent namespace convention
  - read() references resolve to existing SQL files in 140-sql/queries/
  - KV keys referenced via kv('...') are in the known set

Usage:
  python 100-scripts_mlops/ci/check_flows.py [flows_dir]
"""

import sys
import re
from pathlib import Path

import yaml

DEFAULT_FLOWS_DIR = "10-flows/prod"
REQUIRED_FIELDS = {"id", "namespace"}
EXPECTED_NAMESPACE = "projet713"

SQL_BASE_DIR = Path("140-sql")

# Path to the KV keys contract file (single source of truth)
KV_KEYS_CONFIG = Path("kestra_kv_keys.yaml")


def load_kv_keys(config_path: Path = KV_KEYS_CONFIG) -> set[str]:
    """Load known KV keys from the versioned config file."""
    if not config_path.exists():
        print(f"ERROR: KV config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)
    try:
        with open(config_path) as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        print(f"ERROR: invalid YAML in {config_path}: {e}", file=sys.stderr)
        sys.exit(1)
    if not isinstance(data, dict) or "kv_keys" not in data:
        print(f"ERROR: {config_path} must contain a 'kv_keys' list", file=sys.stderr)
        sys.exit(1)
    keys = data["kv_keys"]
    if not isinstance(keys, list) or not all(isinstance(k, str) for k in keys):
        print(f"ERROR: 'kv_keys' in {config_path} must be a list of strings", file=sys.stderr)
        sys.exit(1)
    return set(keys)

# Patterns that indicate hardcoded secrets (case-insensitive)
HARDCODED_SECRET_PATTERNS = [
    re.compile(r'password:\s*["\']?(?!.*\{\{)(?!.*kv\()[A-Za-z0-9]', re.IGNORECASE),
    re.compile(r'secret:\s*["\']?(?!.*\{\{)(?!.*kv\()[A-Za-z0-9]', re.IGNORECASE),
]

# Pattern to extract read('...') references
READ_PATTERN = re.compile(r"read\('([^']+)'\)")

# Pattern to extract kv('...') references
KV_PATTERN = re.compile(r"kv\('([^']+)'\)")


def find_flow_files(base: Path) -> list[Path]:
    return sorted(base.rglob("*.yaml")) + sorted(base.rglob("*.yml"))


def parse_flow(path: Path) -> tuple[dict | None, str | None]:
    """Parse a YAML file. Returns (data, error)."""
    try:
        with open(path) as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            return None, f"not a YAML mapping (got {type(data).__name__})"
        return data, None
    except yaml.YAMLError as e:
        return None, f"YAML syntax error: {e}"


def collect_subflow_refs(data: dict) -> list[str]:
    """Recursively find all flowId references in Subflow tasks."""
    refs = []
    if isinstance(data, dict):
        if data.get("type", "").endswith(".Subflow") and "flowId" in data:
            raw = data["flowId"]
            # skip template expressions — they aren't statically resolvable
            if "{{" not in str(raw):
                refs.append(str(raw))
        for v in data.values():
            refs.extend(collect_subflow_refs(v))
    elif isinstance(data, list):
        for item in data:
            refs.extend(collect_subflow_refs(item))
    return refs


def check_hardcoded_secrets(path: Path) -> list[str]:
    """Scan raw file text for obvious hardcoded credentials."""
    errors = []
    text = path.read_text()
    for pattern in HARDCODED_SECRET_PATTERNS:
        for match in pattern.finditer(text):
            line_no = text[:match.start()].count("\n") + 1
            errors.append(f"line {line_no}: possible hardcoded secret")
    return errors


def validate_flows(flows_dir: Path, known_kv_keys: set[str] | None = None) -> tuple[list[str], list[str]]:
    """Validate all flows. Returns (errors, warnings)."""
    if known_kv_keys is None:
        known_kv_keys = load_kv_keys()
    errors = []
    warnings = []
    files = find_flow_files(flows_dir)

    if not files:
        errors.append(f"no flow files found in {flows_dir}")
        return errors, warnings

    flow_ids: dict[str, Path] = {}
    all_ids: set[str] = set()
    subflow_refs: list[tuple[Path, str]] = []

    for path in files:
        rel = path.relative_to(flows_dir.parent) if flows_dir.parent != path else path
        prefix = str(rel)

        data, err = parse_flow(path)
        if err:
            errors.append(f"{prefix}: {err}")
            continue

        # Required fields
        for field in REQUIRED_FIELDS:
            if field not in data:
                errors.append(f"{prefix}: missing required field '{field}'")

        # tasks or inputs (subflows may only have inputs)
        if "tasks" not in data and "inputs" not in data:
            errors.append(f"{prefix}: missing 'tasks' (and no 'inputs' — not a valid flow)")

        flow_id = data.get("id")
        if flow_id:
            # Duplicate check
            if flow_id in flow_ids:
                errors.append(
                    f"{prefix}: duplicate flow id '{flow_id}' "
                    f"(also in {flow_ids[flow_id]})"
                )
            flow_ids[flow_id] = rel
            all_ids.add(flow_id)

        # Namespace check
        ns = data.get("namespace")
        if ns and ns != EXPECTED_NAMESPACE:
            errors.append(
                f"{prefix}: namespace '{ns}' != expected '{EXPECTED_NAMESPACE}'"
            )

        # Collect subflow references
        subflow_refs.extend((rel, ref) for ref in collect_subflow_refs(data))

        # Check read() references against SQL files
        text = path.read_text()
        for match in READ_PATTERN.finditer(text):
            ref_path = match.group(1)
            resolved = SQL_BASE_DIR / ref_path
            if not resolved.exists():
                errors.append(f"{prefix}: read('{ref_path}') — file not found at {resolved}")

        # Check KV key references
        for match in KV_PATTERN.finditer(text):
            kv_key = match.group(1)
            if kv_key not in known_kv_keys:
                warnings.append(f"{prefix}: kv('{kv_key}') — key not in known set")

        # Hardcoded secrets (warn, don't fail)
        for secret_warn in check_hardcoded_secrets(path):
            warnings.append(f"{prefix}: {secret_warn}")

    # Validate subflow references
    for rel, ref in subflow_refs:
        if ref not in all_ids:
            errors.append(f"{rel}: subflow reference '{ref}' not found in repository")

    return errors, warnings


def main() -> int:
    flows_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(DEFAULT_FLOWS_DIR)
    if not flows_dir.exists():
        print(f"ERROR: flows directory '{flows_dir}' does not exist", file=sys.stderr)
        return 1

    print(f"Checking flows in {flows_dir.resolve()} ...")
    errors, warnings = validate_flows(flows_dir)

    if warnings:
        print(f"\n{len(warnings)} warning(s):\n")
        for w in warnings:
            print(f"  ⚠ {w}")

    if errors:
        print(f"\n{len(errors)} error(s) found:\n")
        for err in errors:
            print(f"  ✗ {err}")
        return 1

    n_files = len(find_flow_files(flows_dir))
    print(f"  ✓ {n_files} flow(s) validated — no issues found")
    return 0


if __name__ == "__main__":
    sys.exit(main())
