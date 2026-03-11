"""Tests for Kestra flows and the CI flow validator.

Unit tests use synthetic flows in tmp dirs.
Integration tests validate the real 10-flows/ directory.
"""

import importlib
import importlib.util
import textwrap
from pathlib import Path

import pytest
import yaml

# ── Import check_flows (package name isn't a valid identifier) ───────────────

_ci_path = Path(__file__).resolve().parents[1] / "100-scripts_mlops" / "ci"
_spec = importlib.util.spec_from_file_location("check_flows", _ci_path / "check_flows.py")
cf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cf)

REPO_ROOT = Path(__file__).resolve().parents[1]
FLOWS_DIR = REPO_ROOT / cf.DEFAULT_FLOWS_DIR

EXPECTED_FLOW_IDS = {
    "elastic_linky_realtime",
    "mlops_linky_forecast_3d",
    "mlops_train_forecast",
    "mqtt_linky_gold",
    "mqtt_linky_ingest",
    "mqtt_linky_silver",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

@pytest.fixture
def flows_dir(tmp_path):
    d = tmp_path / "flows" / "dev"
    d.mkdir(parents=True)
    return d


def _write(path: Path, content: str):
    path.write_text(textwrap.dedent(content))


# ── Unit: YAML syntax ───────────────────────────────────────────────────────

class TestYAMLSyntax:
    def test_valid_yaml(self, flows_dir):
        _write(flows_dir / "ok.yaml", """\
            id: my_flow
            namespace: projet705
            tasks:
              - id: step1
                type: io.kestra.plugin.core.log.Log
        """)
        errors, _ = cf.validate_flows(flows_dir.parent)
        assert errors == []

    def test_invalid_yaml(self, flows_dir):
        (flows_dir / "bad.yaml").write_text("key: [unbalanced")
        errors, _ = cf.validate_flows(flows_dir.parent)
        assert any("YAML syntax error" in e for e in errors)


# ── Unit: required fields ───────────────────────────────────────────────────

class TestRequiredFields:
    def test_missing_id(self, flows_dir):
        _write(flows_dir / "no_id.yaml", """\
            namespace: projet705
            tasks:
              - id: step1
                type: io.kestra.plugin.core.log.Log
        """)
        errors, _ = cf.validate_flows(flows_dir.parent)
        assert any("missing required field 'id'" in e for e in errors)

    def test_missing_namespace(self, flows_dir):
        _write(flows_dir / "no_ns.yaml", """\
            id: my_flow
            tasks:
              - id: step1
                type: io.kestra.plugin.core.log.Log
        """)
        errors, _ = cf.validate_flows(flows_dir.parent)
        assert any("missing required field 'namespace'" in e for e in errors)

    def test_missing_tasks(self, flows_dir):
        _write(flows_dir / "no_tasks.yaml", """\
            id: my_flow
            namespace: projet705
        """)
        errors, _ = cf.validate_flows(flows_dir.parent)
        assert any("missing 'tasks'" in e for e in errors)


# ── Unit: duplicates ────────────────────────────────────────────────────────

class TestDuplicateIDs:
    def test_duplicate_flow_id(self, flows_dir):
        for name in ("a.yaml", "b.yaml"):
            _write(flows_dir / name, """\
                id: same_id
                namespace: projet705
                tasks:
                  - id: step1
                    type: io.kestra.plugin.core.log.Log
            """)
        errors, _ = cf.validate_flows(flows_dir.parent)
        assert any("duplicate flow id" in e for e in errors)


# ── Unit: namespace ─────────────────────────────────────────────────────────

class TestNamespace:
    def test_wrong_namespace(self, flows_dir):
        _write(flows_dir / "wrong.yaml", """\
            id: my_flow
            namespace: other_ns
            tasks:
              - id: step1
                type: io.kestra.plugin.core.log.Log
        """)
        errors, _ = cf.validate_flows(flows_dir.parent)
        assert any("namespace" in e and "other_ns" in e for e in errors)


# ── Unit: subflow references ────────────────────────────────────────────────

class TestSubflowRefs:
    def test_valid_subflow_ref(self, flows_dir):
        _write(flows_dir / "parent.yaml", """\
            id: parent_flow
            namespace: projet705
            tasks:
              - id: call_child
                type: io.kestra.plugin.core.flow.Subflow
                namespace: projet705
                flowId: child_flow
        """)
        _write(flows_dir / "child.yaml", """\
            id: child_flow
            namespace: projet705
            tasks:
              - id: step1
                type: io.kestra.plugin.core.log.Log
        """)
        errors, _ = cf.validate_flows(flows_dir.parent)
        assert errors == []

    def test_broken_subflow_ref(self, flows_dir):
        _write(flows_dir / "parent.yaml", """\
            id: parent_flow
            namespace: projet705
            tasks:
              - id: call_missing
                type: io.kestra.plugin.core.flow.Subflow
                namespace: projet705
                flowId: does_not_exist
        """)
        errors, _ = cf.validate_flows(flows_dir.parent)
        assert any("subflow reference 'does_not_exist' not found" in e for e in errors)

    def test_template_ref_skipped(self, flows_dir):
        """Subflow refs using {{ }} are not statically checked."""
        _write(flows_dir / "dynamic.yaml", """\
            id: dyn_flow
            namespace: projet705
            tasks:
              - id: call_dyn
                type: io.kestra.plugin.core.flow.Subflow
                namespace: projet705
                flowId: "{{ inputs.target }}"
        """)
        errors, _ = cf.validate_flows(flows_dir.parent)
        assert errors == []


# ── Integration: real flow files ─────────────────────────────────────────────

def _skip_if_no_flows():
    if not FLOWS_DIR.exists():
        pytest.skip(f"{cf.DEFAULT_FLOWS_DIR}/ not found")


@pytest.fixture
def repo_flows():
    _skip_if_no_flows()
    files = cf.find_flow_files(FLOWS_DIR)
    return {f.stem: yaml.safe_load(f.read_text()) for f in files}


class TestRepoFlowFiles:
    """Validate the actual flow YAML files in the repository."""

    def test_expected_flows_exist(self, repo_flows):
        missing = EXPECTED_FLOW_IDS - set(repo_flows.keys())
        assert not missing, f"Missing expected flows: {missing}"

    def test_all_parse_as_yaml(self, repo_flows):
        for name, data in repo_flows.items():
            assert isinstance(data, dict), f"{name} did not parse as a YAML mapping"

    def test_all_have_id(self, repo_flows):
        for name, data in repo_flows.items():
            assert "id" in data, f"{name} missing 'id'"
            assert data["id"] == name, f"{name}: id '{data['id']}' != filename"

    def test_all_have_namespace(self, repo_flows):
        for name, data in repo_flows.items():
            assert data.get("namespace") == cf.EXPECTED_NAMESPACE, (
                f"{name}: namespace should be '{cf.EXPECTED_NAMESPACE}'"
            )

    def test_all_have_tasks(self, repo_flows):
        for name, data in repo_flows.items():
            assert "tasks" in data, f"{name} missing 'tasks'"
            assert len(data["tasks"]) > 0, f"{name} has empty tasks list"

    def test_no_duplicate_ids(self, repo_flows):
        ids = [data["id"] for data in repo_flows.values()]
        assert len(ids) == len(set(ids)), f"Duplicate flow IDs: {ids}"

    def test_all_have_triggers(self, repo_flows):
        for name, data in repo_flows.items():
            if "inputs" in data and "triggers" not in data:
                continue  # subflows are triggered by parent flows
            assert "triggers" in data, f"{name} missing 'triggers'"
