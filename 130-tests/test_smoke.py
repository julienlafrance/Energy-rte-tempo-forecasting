"""Tests for the CD smoke test script (smoke_test_prod.py).

Unit tests verify config loading and check logic using mocks.
No Kestra server or network access required.
"""

import importlib.util
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

# ── Import smoke_test_prod ───────────────────────────────────────────────────

_deploy_path = Path(__file__).resolve().parents[1] / "95-ci-cd" / "deploy"
_spec = importlib.util.spec_from_file_location("smoke_test_prod", _deploy_path / "smoke_test_prod.py")
st = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(st)

REPO_ROOT = Path(__file__).resolve().parents[1]
REAL_CONFIG = REPO_ROOT / "95-ci-cd" / "config" / "deploy_smoke_tests.yaml"


# ── Config loading ───────────────────────────────────────────────────────────

class TestLoadConfig:
    def test_loads_real_config(self):
        config = st.load_config(REAL_CONFIG)
        assert "expected_flows" in config
        assert "critical_kv_keys" in config
        assert isinstance(config["expected_flows"], list)
        assert isinstance(config["critical_kv_keys"], list)
        assert len(config["expected_flows"]) > 0
        assert len(config["critical_kv_keys"]) > 0

    def test_missing_config_exits(self, tmp_path):
        with pytest.raises(SystemExit):
            st.load_config(tmp_path / "nonexistent.yaml")

    def test_invalid_yaml_exits(self, tmp_path):
        bad = tmp_path / "bad.yaml"
        bad.write_text("not: a\nvalid: [")
        with pytest.raises(SystemExit):
            st.load_config(bad)

    def test_missing_expected_flows_exits(self, tmp_path):
        cfg = tmp_path / "no_flows.yaml"
        cfg.write_text(yaml.dump({"critical_kv_keys": ["PG_JDBC"]}))
        with pytest.raises(SystemExit):
            st.load_config(cfg)

    def test_missing_critical_kv_keys_exits(self, tmp_path):
        cfg = tmp_path / "no_kv.yaml"
        cfg.write_text(yaml.dump({"expected_flows": ["my_flow"]}))
        with pytest.raises(SystemExit):
            st.load_config(cfg)


# ── Config content ───────────────────────────────────────────────────────────

class TestConfigContent:
    """Validate the actual deploy_smoke_tests.yaml content."""

    def test_expected_flows_match_repo(self):
        """Flows in smoke config should be a subset of actual flow files."""
        config = st.load_config(REAL_CONFIG)
        flows_dir = REPO_ROOT / "10-flows" / "prod"
        if not flows_dir.exists():
            pytest.skip("10-flows/prod/ not found")
        actual_ids = {f.stem for f in flows_dir.glob("*.yaml")}
        for flow_id in config["expected_flows"]:
            assert flow_id in actual_ids, (
                f"Smoke test expects flow '{flow_id}' but no matching YAML in 10-flows/prod/"
            )

    def test_critical_kv_keys_subset_of_contract(self):
        """Critical KV keys should be a subset of kestra_kv_keys.yaml."""
        config = st.load_config(REAL_CONFIG)
        kv_contract = REPO_ROOT / "kestra_kv_keys.yaml"
        if not kv_contract.exists():
            pytest.skip("kestra_kv_keys.yaml not found")
        with open(kv_contract) as f:
            all_keys = set(yaml.safe_load(f).get("kv_keys", []))
        for key in config["critical_kv_keys"]:
            assert key in all_keys, (
                f"Smoke test expects KV '{key}' but it's not in kestra_kv_keys.yaml"
            )


# ── Check functions (mocked HTTP) ───────────────────────────────────────────

class TestCheckFlows:
    @patch.object(st, "_http_status", return_value=200)
    def test_all_pass(self, mock_http):
        failures = st.check_flows("http://fake", "ns", "u", "p", ["flow_a", "flow_b"])
        assert failures == 0
        assert mock_http.call_count == 2

    @patch.object(st, "_http_status", return_value=404)
    def test_all_fail(self, mock_http):
        failures = st.check_flows("http://fake", "ns", "u", "p", ["flow_a"])
        assert failures == 1

    @patch.object(st, "_http_status", side_effect=[200, 404, 200])
    def test_partial_failure(self, mock_http):
        failures = st.check_flows("http://fake", "ns", "u", "p", ["a", "b", "c"])
        assert failures == 1


class TestCheckNamespaceFiles:
    @patch.object(st, "_http_status", return_value=200)
    def test_sql_files_found(self, mock_http, tmp_path):
        queries = tmp_path / "queries"
        queries.mkdir()
        (queries / "a.sql").write_text("SELECT 1;")
        (queries / "b.sql").write_text("SELECT 2;")
        failures = st.check_namespace_files("http://fake", "ns", "u", "p",
                                            str(tmp_path))
        assert failures == 0
        assert mock_http.call_count == 2

    def test_no_sql_files(self, tmp_path):
        queries = tmp_path / "queries"
        queries.mkdir()
        failures = st.check_namespace_files("http://fake", "ns", "u", "p",
                                            str(tmp_path))
        assert failures == 0


class TestCheckKVKeys:
    @patch.object(st, "_http_status", return_value=200)
    def test_all_accessible(self, mock_http):
        failures = st.check_kv_keys("http://fake", "ns", "u", "p",
                                    ["PG_JDBC", "MQTT_SERVER"])
        assert failures == 0

    @patch.object(st, "_http_status", return_value=200)
    def test_all_accessible_strict(self, mock_http):
        failures = st.check_kv_keys("http://fake", "ns", "u", "p",
                                    ["PG_JDBC"], strict=True)
        assert failures == 0

    @patch.object(st, "_http_status", return_value=404)
    def test_missing_in_strict_mode_fails(self, mock_http):
        failures = st.check_kv_keys("http://fake", "ns", "u", "p",
                                    ["PG_JDBC"], strict=True)
        assert failures == 1

    @patch.object(st, "_http_status", return_value=404)
    def test_missing_in_default_mode_warns(self, mock_http):
        failures = st.check_kv_keys("http://fake", "ns", "u", "p",
                                    ["PG_JDBC"], strict=False)
        assert failures == 0


class TestMain:
    @patch("sys.argv", ["smoke_test_prod.py"])
    @patch.dict("os.environ", {
        "KESTRA_SERVER": "", "KESTRA_NAMESPACE": "",
        "KESTRA_USER": "", "KESTRA_PASS": "",
    })
    def test_missing_env_vars_returns_1(self):
        assert st.main() == 1
