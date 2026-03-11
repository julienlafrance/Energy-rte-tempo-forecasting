"""Sanity tests for the deployment script — no execution, no network calls."""

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_SCRIPT = REPO_ROOT / "100-scripts_mlops" / "deploy" / "deploy_flows.sh"


class TestDeployScriptExists:
    def test_script_exists(self):
        assert DEPLOY_SCRIPT.exists(), f"Deploy script not found: {DEPLOY_SCRIPT}"

    def test_script_is_executable(self):
        assert os.access(DEPLOY_SCRIPT, os.X_OK), "deploy_flows.sh should be executable"

    def test_has_shebang(self):
        first_line = DEPLOY_SCRIPT.read_text().splitlines()[0]
        assert first_line.startswith("#!/"), "Script should have a shebang line"


class TestDeployScriptSafety:
    def test_strict_mode(self):
        text = DEPLOY_SCRIPT.read_text()
        assert "set -euo pipefail" in text

    def test_credentials_from_variables(self):
        text = DEPLOY_SCRIPT.read_text()
        assert "KESTRA_ADMIN_USER" in text
        assert "KESTRA_ADMIN_PASS" in text
        for line in text.splitlines():
            if "-u " in line:
                assert "${KESTRA_ADMIN_USER}" in line or "$KESTRA_ADMIN_USER" in line

    def test_kestra_url_configurable(self):
        text = DEPLOY_SCRIPT.read_text()
        assert "KESTRA_URL" in text


class TestDeployScriptLogic:
    def test_validates_before_deploying(self):
        text = DEPLOY_SCRIPT.read_text()
        assert text.find("/validate") < text.find("-X PUT"), (
            "Validation should happen before deployment"
        )

    def test_iterates_over_yaml_files(self):
        text = DEPLOY_SCRIPT.read_text()
        assert "*.yaml" in text or "*.yml" in text

    def test_uses_nullglob(self):
        text = DEPLOY_SCRIPT.read_text()
        assert "nullglob" in text, "Script should use nullglob to handle empty globs"
