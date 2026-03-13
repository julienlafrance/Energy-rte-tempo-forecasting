"""Tests for repo_structure.yaml — config loading and sync with workflows.

Ensures the central config is valid and that workflow env blocks
stay in sync with the source of truth.
"""

import importlib.util
import re
from pathlib import Path

import pytest
import yaml

# ── Import load_config ───────────────────────────────────────────────────────

_config_path = Path(__file__).resolve().parents[1] / "95-ci-cd" / "config"
_spec = importlib.util.spec_from_file_location("load_config", _config_path / "load_config.py")
lc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lc)

REPO_ROOT = Path(__file__).resolve().parents[1]
STRUCTURE_PATH = REPO_ROOT / "95-ci-cd" / "config" / "repo_structure.yaml"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _load_workflow(name: str) -> dict:
    path = REPO_ROOT / ".github" / "workflows" / name
    if not path.exists():
        pytest.skip(f"{name} not found")
    with open(path) as f:
        return yaml.safe_load(f)


# ── Config validity ──────────────────────────────────────────────────────────

class TestRepoStructureValid:
    def test_loads_successfully(self):
        cfg = lc.load_repo_structure()
        assert isinstance(cfg, dict)

    def test_has_required_sections(self):
        cfg = lc.load_repo_structure()
        for section in ("directories", "kestra",
                        "docker", "infra", "sync", "config_files"):
            assert section in cfg, f"Missing section '{section}'"

    def test_directories_non_empty(self):
        dirs = lc.load_repo_structure()["directories"]
        for key in ("flows", "sql", "scripts", "cicd", "api", "webapp"):
            assert dirs.get(key), f"directories.{key} is empty"

    def test_kestra_values(self):
        k = lc.load_repo_structure()["kestra"]
        assert k.get("namespace")
        assert k.get("dev_server", "").startswith("http")
        assert k.get("prod_server", "").startswith("http")

    def test_docker_build_contexts(self):
        docker = lc.load_repo_structure()["docker"]
        assert docker.get("api_build_context"), "docker.api_build_context is empty"
        assert docker.get("webapp_build_context"), "docker.webapp_build_context is empty"

    def test_infra_charts_dir(self):
        infra = lc.load_repo_structure()["infra"]
        assert infra.get("charts_dir"), "infra.charts_dir is empty"

    def test_all_config_directories_exist(self):
        dirs = lc.load_repo_structure()["directories"]
        for key, rel_path in dirs.items():
            assert (REPO_ROOT / rel_path).exists(), (
                f"directories.{key} = '{rel_path}' does not exist"
            )

    def test_docker_build_contexts_exist(self):
        docker = lc.load_repo_structure()["docker"]
        for key in ("api_build_context", "webapp_build_context"):
            path = docker[key]
            assert (REPO_ROOT / path).exists(), (
                f"docker.{key} = '{path}' does not exist"
            )

    def test_infra_charts_dir_exists(self):
        path = lc.load_repo_structure()["infra"]["charts_dir"]
        assert (REPO_ROOT / path).exists(), (
            f"infra.charts_dir = '{path}' does not exist"
        )

    def test_sync_has_target_base_path(self):
        sync = lc.load_repo_structure()["sync"]
        assert sync.get("target_base_path"), "sync.target_base_path is empty"

    def test_sync_has_directories(self):
        sync = lc.load_repo_structure()["sync"]
        assert isinstance(sync.get("directories"), list), "sync.directories must be a list"
        assert len(sync["directories"]) > 0, "sync.directories is empty"

    def test_sync_directories_exist(self):
        sync = lc.load_repo_structure()["sync"]
        for rel_path in sync["directories"]:
            assert (REPO_ROOT / rel_path).exists(), (
                f"sync directory '{rel_path}' does not exist"
            )


# ── Workflow sync: ci.yml ────────────────────────────────────────────────────

class TestCIWorkflowSync:
    """Verify ci.yml env vars match repo_structure.yaml."""

    def test_flows_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("ci.yml")
        assert wf["env"]["FLOWS_DIR"] == cfg["directories"]["flows"]

    def test_sql_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("ci.yml")
        assert wf["env"]["SQL_DIR"] == cfg["directories"]["sql"]

    def test_scripts_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("ci.yml")
        assert wf["env"]["SCRIPTS_DIR"] == cfg["directories"]["scripts"]

    def test_api_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("ci.yml")
        assert wf["env"]["API_DIR"] == cfg["directories"]["api"]

    def test_webapp_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("ci.yml")
        assert wf["env"]["WEBAPP_DIR"] == cfg["directories"]["webapp"]

    def test_kestra_dev_server(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("ci.yml")
        assert wf["env"]["KESTRA_DEV_SERVER"] == cfg["kestra"]["dev_server"]

    def test_flow_scripts_dir(self):
        """SCRIPTS_DIR alone now points to the business scripts directory."""
        cfg = lc.load_repo_structure()
        wf = _load_workflow("ci.yml")
        assert wf["env"]["SCRIPTS_DIR"] == cfg["directories"]["scripts"]
        # FLOW_SCRIPTS_DIR no longer exists — scripts are at root of SCRIPTS_DIR
        assert "FLOW_SCRIPTS_DIR" not in wf["env"]

    def test_no_infra_env_vars(self):
        """Docker builds and Helm lint are infra team responsibility, not CI."""
        wf = _load_workflow("ci.yml")
        env = wf.get("env", {})
        assert "API_BUILD_CTX" not in env, "API_BUILD_CTX should not be in ci.yml"
        assert "INFRA_CHARTS_DIR" not in env, "INFRA_CHARTS_DIR should not be in ci.yml"


# ── Workflow sync: deploy.yml ────────────────────────────────────────────────

class TestDeployWorkflowSync:
    """Verify deploy.yml env vars match repo_structure.yaml."""

    def test_kestra_namespace(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("deploy.yml")
        assert wf["env"]["KESTRA_NAMESPACE"] == cfg["kestra"]["namespace"]

    def test_kestra_server(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("deploy.yml")
        assert wf["env"]["KESTRA_SERVER"] == cfg["kestra"]["prod_server"]

    def test_flows_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("deploy.yml")
        assert wf["env"]["FLOWS_DIR"] == cfg["directories"]["flows"]

    def test_sql_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("deploy.yml")
        assert wf["env"]["SQL_DIR"] == cfg["directories"]["sql"]

    def test_scripts_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("deploy.yml")
        assert wf["env"]["SCRIPTS_DIR"] == cfg["directories"]["scripts"]

    def test_flow_scripts_dir(self):
        """SCRIPTS_DIR alone points to business scripts. No FLOW_SCRIPTS_DIR."""
        cfg = lc.load_repo_structure()
        wf = _load_workflow("deploy.yml")
        assert wf["env"]["SCRIPTS_DIR"] == cfg["directories"]["scripts"]
        assert "FLOW_SCRIPTS_DIR" not in wf["env"]

    def test_no_docker_env_vars(self):
        """deploy.yml should NOT have API_IMAGE or WEBAPP_IMAGE env vars.

        Docker build/push is NOT part of the CD pipeline.
        """
        wf = _load_workflow("deploy.yml")
        env = wf.get("env", {})
        assert "API_IMAGE" not in env, "API_IMAGE should not be in deploy.yml env"
        assert "WEBAPP_IMAGE" not in env, "WEBAPP_IMAGE should not be in deploy.yml env"

    def test_sync_target(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("deploy.yml")
        assert wf["env"]["SYNC_TARGET"] == cfg["sync"]["target_base_path"]

    def test_api_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("deploy.yml")
        assert wf["env"]["API_DIR"] == cfg["directories"]["api"]

    def test_webapp_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("deploy.yml")
        assert wf["env"]["WEBAPP_DIR"] == cfg["directories"]["webapp"]


# ── Workflow sync: sync-dev.yml ──────────────────────────────────────────────

class TestSyncDevWorkflowSync:
    """Verify sync-dev.yml env vars match repo_structure.yaml."""

    def test_sync_target(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("sync-dev.yml")
        assert wf["env"]["SYNC_TARGET"] == cfg["sync"]["target_base_path"]

    def test_flows_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("sync-dev.yml")
        assert wf["env"]["FLOWS_DIR"] == cfg["directories"]["flows"]

    def test_sql_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("sync-dev.yml")
        assert wf["env"]["SQL_DIR"] == cfg["directories"]["sql"]

    def test_scripts_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("sync-dev.yml")
        assert wf["env"]["SCRIPTS_DIR"] == cfg["directories"]["scripts"]

    def test_api_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("sync-dev.yml")
        assert wf["env"]["API_DIR"] == cfg["directories"]["api"]

    def test_webapp_dir(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("sync-dev.yml")
        assert wf["env"]["WEBAPP_DIR"] == cfg["directories"]["webapp"]

    def test_kestra_namespace(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("sync-dev.yml")
        assert wf["env"]["KESTRA_NAMESPACE"] == cfg["kestra"]["namespace"]

    def test_kestra_dev_server(self):
        cfg = lc.load_repo_structure()
        wf = _load_workflow("sync-dev.yml")
        assert wf["env"]["KESTRA_DEV_SERVER"] == cfg["kestra"]["dev_server"]

    def test_workflow_run_trigger(self):
        wf = _load_workflow("sync-dev.yml")
        triggers = wf.get(True) or wf.get("on", {})
        wr = triggers.get("workflow_run", {})
        assert "ci" in wr.get("workflows", []), "workflow_run must reference 'ci'"
        assert "completed" in wr.get("types", []), "workflow_run must trigger on 'completed'"

    def test_workflow_dispatch_trigger(self):
        wf = _load_workflow("sync-dev.yml")
        triggers = wf.get(True) or wf.get("on", {})
        assert "workflow_dispatch" in triggers, "sync-dev.yml must support workflow_dispatch"

    def test_runs_on_dev(self):
        wf = _load_workflow("sync-dev.yml")
        job = wf["jobs"]["sync-and-deploy"]
        assert "self-hosted" in job["runs-on"]
        assert "dev" in job["runs-on"]

    def test_no_rollback_job(self):
        """DEV must NOT have a rollback job."""
        wf = _load_workflow("sync-dev.yml")
        assert "rollback" not in wf["jobs"], "sync-dev.yml must NOT have a rollback job"

    def test_flows_deploy_non_destructive(self):
        """Verify flows are deployed with delete: false."""
        wf = _load_workflow("sync-dev.yml")
        job = wf["jobs"]["sync-and-deploy"]
        deploy_steps = [
            s for s in job["steps"]
            if s.get("uses", "").startswith("kestra-io/deploy-action")
            and s.get("with", {}).get("resource") == "flow"
        ]
        assert deploy_steps, "Must have a deploy-action step for flows"
        for step in deploy_steps:
            assert step["with"].get("delete") is False, (
                "deploy-action for flows must use delete: false"
            )


# ── Config files validity ────────────────────────────────────────────────────

class TestConfigFiles:
    """Validate that referenced config files exist and are well-formed."""

    def test_smoke_tests_config_exists(self):
        cfg = lc.load_repo_structure()
        path = REPO_ROOT / cfg["config_files"]["smoke_tests"]
        assert path.exists(), f"Config file not found: {path}"

    def test_apps_smoke_tests_config_exists(self):
        cfg = lc.load_repo_structure()
        path = REPO_ROOT / cfg["config_files"]["apps_smoke_tests"]
        assert path.exists(), f"Config file not found: {path}"

    def test_apps_smoke_tests_valid_yaml(self):
        cfg = lc.load_repo_structure()
        path = REPO_ROOT / cfg["config_files"]["apps_smoke_tests"]
        with open(path) as f:
            data = yaml.safe_load(f)
        assert isinstance(data, dict)

    def test_apps_smoke_has_dev_and_prod(self):
        cfg = lc.load_repo_structure()
        path = REPO_ROOT / cfg["config_files"]["apps_smoke_tests"]
        with open(path) as f:
            data = yaml.safe_load(f)
        for env_name in ("dev", "prod"):
            assert env_name in data, f"Missing '{env_name}' section"
            assert "endpoints" in data[env_name], f"Missing 'endpoints' in {env_name}"
            assert isinstance(data[env_name]["endpoints"], list)
            assert len(data[env_name]["endpoints"]) > 0

    def test_apps_smoke_endpoints_have_required_fields(self):
        cfg = lc.load_repo_structure()
        path = REPO_ROOT / cfg["config_files"]["apps_smoke_tests"]
        with open(path) as f:
            data = yaml.safe_load(f)
        for env_name in ("dev", "prod"):
            for ep in data[env_name]["endpoints"]:
                for key in ("name", "url", "health_path"):
                    assert key in ep, f"Endpoint missing '{key}' in {env_name}: {ep}"

    def test_helm_charts_have_chart_yaml(self):
        cfg = lc.load_repo_structure()
        charts_dir = REPO_ROOT / cfg["infra"]["charts_dir"]
        charts_found = 0
        for child in sorted(charts_dir.iterdir()):
            if child.is_dir() and (child / "Chart.yaml").exists():
                charts_found += 1
        assert charts_found >= 1, f"No Helm charts found in {charts_dir}"
