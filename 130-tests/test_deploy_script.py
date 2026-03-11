"""Sanity tests for the deployment script — no execution, no network calls."""

import os
from pathlib import Path

DEPLOY_SCRIPT = Path(__file__).resolve().parents[1] / "100-scripts_mlops" / "deploy" / "deploy_flows.sh"


class TestDeployScriptExists:
    def test_script_exists(self):
        assert DEPLOY_SCRIPT.exists(), f"Deploy script not found: {DEPLOY_SCRIPT}"

    def test_script_is_executable(self):
        assert os.access(DEPLOY_SCRIPT, os.X_OK), "deploy_flows.sh should be executable"


class TestDeployScriptContent:
    def test_has_strict_mode(self):
        text = DEPLOY_SCRIPT.read_text()
        assert "set -euo pipefail" in text, "Script must use 'set -euo pipefail' for safety"

    def test_uses_kestra_url_variable(self):
        text = DEPLOY_SCRIPT.read_text()
        assert "KESTRA_URL" in text, "Script should reference KESTRA_URL"

    def test_uses_credential_variables(self):
        text = DEPLOY_SCRIPT.read_text()
        assert "KESTRA_ADMIN_USER" in text, "Script should reference KESTRA_ADMIN_USER"
        assert "KESTRA_ADMIN_PASS" in text, "Script should reference KESTRA_ADMIN_PASS"

    def test_no_hardcoded_credentials(self):
        text = DEPLOY_SCRIPT.read_text()
        # Ensure credentials are read from variables, not hardcoded
        assert "curl" in text, "Script should use curl to call the Kestra API"
        # The -u flag should reference variables, not literal values
        for line in text.splitlines():
            if "-u " in line:
                assert "${KESTRA_ADMIN_USER}" in line or "$KESTRA_ADMIN_USER" in line, (
                    "curl -u should use KESTRA_ADMIN_USER variable"
                )

    def test_validates_before_deploying(self):
        text = DEPLOY_SCRIPT.read_text()
        # The script should call the validate endpoint before the update endpoint
        validate_pos = text.find("/validate")
        put_pos = text.find("-X PUT")
        assert validate_pos != -1, "Script should call the validate API endpoint"
        assert put_pos != -1, "Script should call the update (PUT) API endpoint"
        assert validate_pos < put_pos, "Validation should happen before deployment"
