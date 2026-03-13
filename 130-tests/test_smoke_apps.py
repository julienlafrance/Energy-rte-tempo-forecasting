"""Tests for the app smoke test script (smoke_test_apps.py).

Unit tests verify config loading and health check logic using mocks.
No network access required.
"""

import importlib.util
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

# ── Import smoke_test_apps ───────────────────────────────────────────────────

_deploy_path = Path(__file__).resolve().parents[1] / "95-ci-cd" / "deploy"
_spec = importlib.util.spec_from_file_location(
    "smoke_test_apps", _deploy_path / "smoke_test_apps.py"
)
sta = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sta)

REPO_ROOT = Path(__file__).resolve().parents[1]
REAL_CONFIG = REPO_ROOT / "95-ci-cd" / "config" / "deploy_apps_smoke_tests.yaml"


# ── Config loading ───────────────────────────────────────────────────────────

class TestLoadConfig:
    def test_loads_real_config(self):
        config = sta.load_config(REAL_CONFIG)
        assert "dev" in config
        assert "prod" in config
        assert isinstance(config["dev"]["endpoints"], list)
        assert isinstance(config["prod"]["endpoints"], list)

    def test_missing_config_exits(self, tmp_path):
        with pytest.raises(SystemExit):
            sta.load_config(tmp_path / "nonexistent.yaml")

    def test_invalid_yaml_exits(self, tmp_path):
        bad = tmp_path / "bad.yaml"
        bad.write_text("not: a\nvalid: [")
        with pytest.raises(SystemExit):
            sta.load_config(bad)

    def test_missing_dev_section_exits(self, tmp_path):
        cfg = tmp_path / "no_dev.yaml"
        cfg.write_text(yaml.dump({
            "prod": {"endpoints": [{"name": "API", "url": "http://x", "health_path": "/h"}]},
        }))
        with pytest.raises(SystemExit):
            sta.load_config(cfg)

    def test_missing_prod_section_exits(self, tmp_path):
        cfg = tmp_path / "no_prod.yaml"
        cfg.write_text(yaml.dump({
            "dev": {"endpoints": [{"name": "API", "url": "http://x", "health_path": "/h"}]},
        }))
        with pytest.raises(SystemExit):
            sta.load_config(cfg)

    def test_missing_endpoint_field_exits(self, tmp_path):
        cfg = tmp_path / "bad_ep.yaml"
        cfg.write_text(yaml.dump({
            "dev": {"endpoints": [{"name": "API"}]},
            "prod": {"endpoints": [{"name": "API", "url": "http://x", "health_path": "/h"}]},
        }))
        with pytest.raises(SystemExit):
            sta.load_config(cfg)


# ── Config content ───────────────────────────────────────────────────────────

class TestConfigContent:
    def test_dev_endpoints_have_required_fields(self):
        config = sta.load_config(REAL_CONFIG)
        for ep in config["dev"]["endpoints"]:
            assert "name" in ep
            assert "url" in ep
            assert "health_path" in ep

    def test_prod_endpoints_have_required_fields(self):
        config = sta.load_config(REAL_CONFIG)
        for ep in config["prod"]["endpoints"]:
            assert "name" in ep
            assert "url" in ep
            assert "health_path" in ep

    def test_at_least_one_required_endpoint_per_env(self):
        config = sta.load_config(REAL_CONFIG)
        for env_name in ("dev", "prod"):
            required = [ep for ep in config[env_name]["endpoints"]
                        if ep.get("required", True)]
            assert len(required) >= 1, (
                f"At least one endpoint should be required in {env_name}"
            )


# ── Check endpoints (mocked HTTP) ───────────────────────────────────────────

class TestCheckEndpoints:
    @patch.object(sta, "http_health_check", return_value=200)
    def test_all_pass(self, mock_http):
        endpoints = [
            {"name": "API", "url": "http://fake:8000", "health_path": "/health", "required": True},
            {"name": "Webapp", "url": "http://fake:8501", "health_path": "/_stcore/health", "required": True},
        ]
        failures = sta.check_endpoints(endpoints)
        assert failures == 0
        assert mock_http.call_count == 2

    @patch.object(sta, "http_health_check", return_value=0)
    def test_required_failure(self, mock_http):
        endpoints = [
            {"name": "API", "url": "http://fake:8000", "health_path": "/health", "required": True},
        ]
        failures = sta.check_endpoints(endpoints)
        assert failures == 1

    @patch.object(sta, "http_health_check", return_value=0)
    def test_non_required_failure_is_warning(self, mock_http):
        endpoints = [
            {"name": "Webapp", "url": "http://fake:8501", "health_path": "/_stcore/health", "required": False},
        ]
        failures = sta.check_endpoints(endpoints)
        assert failures == 0

    @patch.object(sta, "http_health_check", side_effect=[200, 503])
    def test_partial_failure(self, mock_http):
        endpoints = [
            {"name": "API", "url": "http://fake:8000", "health_path": "/health", "required": True},
            {"name": "Webapp", "url": "http://fake:8501", "health_path": "/_stcore/health", "required": True},
        ]
        failures = sta.check_endpoints(endpoints)
        assert failures == 1


class TestMain:
    @patch.object(sta, "check_endpoints", return_value=0)
    @patch.object(sta, "load_config", return_value={
        "dev": {"endpoints": []},
        "prod": {"endpoints": []},
    })
    def test_dev_success_returns_0(self, mock_cfg, mock_check):
        assert sta.main(["--env", "dev"]) == 0

    @patch.object(sta, "check_endpoints", return_value=0)
    @patch.object(sta, "load_config", return_value={
        "dev": {"endpoints": []},
        "prod": {"endpoints": []},
    })
    def test_prod_success_returns_0(self, mock_cfg, mock_check):
        assert sta.main(["--env", "prod"]) == 0

    @patch.object(sta, "check_endpoints", return_value=2)
    @patch.object(sta, "load_config", return_value={
        "dev": {"endpoints": []},
        "prod": {"endpoints": []},
    })
    def test_failures_returns_1(self, mock_cfg, mock_check):
        assert sta.main(["--env", "prod"]) == 1
