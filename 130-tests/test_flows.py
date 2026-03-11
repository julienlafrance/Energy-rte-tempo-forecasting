"""Tests for the Kestra flow validator — fully local and deterministic."""

import importlib
import importlib.util
import textwrap
from pathlib import Path

import pytest

# 100-scripts_mlops is not a valid Python identifier, so use importlib
_ci_path = Path(__file__).resolve().parents[1] / "100-scripts_mlops" / "ci"
_spec = importlib.util.spec_from_file_location("check_flows", _ci_path / "check_flows.py")
cf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cf)


@pytest.fixture
def flows_dir(tmp_path):
    """Create a temporary flows directory with valid flows."""
    d = tmp_path / "flows" / "dev"
    d.mkdir(parents=True)
    return d


def _write(path: Path, content: str):
    path.write_text(textwrap.dedent(content))


# ── YAML syntax ──────────────────────────────────────────────────────────────

class TestYAMLSyntax:
    def test_valid_yaml(self, flows_dir):
        _write(flows_dir / "ok.yaml", """\
            id: my_flow
            namespace: projet705
            tasks:
              - id: step1
                type: io.kestra.plugin.core.log.Log
                message: hello
        """)
        assert cf.validate_flows(flows_dir.parent) == []

    def test_invalid_yaml(self, flows_dir):
        (flows_dir / "bad.yaml").write_text("key: [unbalanced")
        errors = cf.validate_flows(flows_dir.parent)
        assert len(errors) == 1
        assert "YAML syntax error" in errors[0]


# ── Required fields ──────────────────────────────────────────────────────────

class TestRequiredFields:
    def test_missing_id(self, flows_dir):
        _write(flows_dir / "no_id.yaml", """\
            namespace: projet705
            tasks:
              - id: step1
                type: io.kestra.plugin.core.log.Log
        """)
        errors = cf.validate_flows(flows_dir.parent)
        assert any("missing required field 'id'" in e for e in errors)

    def test_missing_namespace(self, flows_dir):
        _write(flows_dir / "no_ns.yaml", """\
            id: my_flow
            tasks:
              - id: step1
                type: io.kestra.plugin.core.log.Log
        """)
        errors = cf.validate_flows(flows_dir.parent)
        assert any("missing required field 'namespace'" in e for e in errors)

    def test_missing_tasks(self, flows_dir):
        _write(flows_dir / "no_tasks.yaml", """\
            id: my_flow
            namespace: projet705
        """)
        errors = cf.validate_flows(flows_dir.parent)
        assert any("missing 'tasks'" in e for e in errors)

    def test_subflow_with_inputs_no_tasks(self, flows_dir):
        """A subflow that has inputs + tasks is valid."""
        _write(flows_dir / "sub.yaml", """\
            id: my_sub
            namespace: projet705
            inputs:
              - id: metric
                type: STRING
            tasks:
              - id: step1
                type: io.kestra.plugin.core.log.Log
        """)
        assert cf.validate_flows(flows_dir.parent) == []


# ── Duplicate IDs ────────────────────────────────────────────────────────────

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
        errors = cf.validate_flows(flows_dir.parent)
        assert any("duplicate flow id" in e for e in errors)


# ── Namespace ────────────────────────────────────────────────────────────────

class TestNamespace:
    def test_wrong_namespace(self, flows_dir):
        _write(flows_dir / "wrong.yaml", """\
            id: my_flow
            namespace: other_ns
            tasks:
              - id: step1
                type: io.kestra.plugin.core.log.Log
        """)
        errors = cf.validate_flows(flows_dir.parent)
        assert any("namespace" in e and "other_ns" in e for e in errors)


# ── Subflow references ───────────────────────────────────────────────────────

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
        assert cf.validate_flows(flows_dir.parent) == []

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
        errors = cf.validate_flows(flows_dir.parent)
        assert any("subflow reference 'does_not_exist' not found" in e for e in errors)

    def test_template_subflow_ref_skipped(self, flows_dir):
        """Subflow refs using {{ }} templates are not statically checked."""
        _write(flows_dir / "dynamic.yaml", """\
            id: dyn_flow
            namespace: projet705
            tasks:
              - id: call_dyn
                type: io.kestra.plugin.core.flow.Subflow
                namespace: projet705
                flowId: "{{ inputs.target }}"
        """)
        assert cf.validate_flows(flows_dir.parent) == []


# ── Hardcoded secrets ────────────────────────────────────────────────────────

class TestHardcodedSecrets:
    def test_hardcoded_password_detected(self, flows_dir):
        _write(flows_dir / "creds.yaml", """\
            id: bad_flow
            namespace: projet705
            tasks:
              - id: db
                type: io.kestra.plugin.jdbc.postgresql.Query
                password: SuperSecret123
        """)
        errors = cf.validate_flows(flows_dir.parent)
        assert any("hardcoded secret" in e for e in errors)

    def test_kv_password_ok(self, flows_dir):
        _write(flows_dir / "safe.yaml", """\
            id: safe_flow
            namespace: projet705
            tasks:
              - id: db
                type: io.kestra.plugin.jdbc.postgresql.Query
                password: "{{ kv('PG_PASS') }}"
        """)
        errors = cf.validate_flows(flows_dir.parent)
        assert not any("hardcoded secret" in e for e in errors)


# ── Integration: validate actual repo flows ──────────────────────────────────

class TestRepoFlows:
    """Run the checker against the real flows/ directory in this repo."""

    def test_repo_flows_are_valid(self):
        repo_flows = Path(__file__).resolve().parents[1] / "150-flows"
        if not repo_flows.exists():
            pytest.skip("flows/ directory not found")
        errors = cf.validate_flows(repo_flows)
        assert errors == [], f"Flow validation errors:\n" + "\n".join(f"  ✗ {e}" for e in errors)
