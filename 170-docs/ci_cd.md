# CI/CD Pipeline

## Overview

This project uses GitHub Actions for continuous integration and continuous deployment.

| Stage | Workflow | Trigger | Runner |
|-------|----------|---------|--------|
| CI    | `.github/workflows/validate.yml` | push / PR to `main` | `ubuntu-latest` |
| CD    | `.github/workflows/deploy.yml`   | push to `prod` / manual | `self-hosted` (production VM) |

---

## CI — Validation (`validate.yml`)

Runs on every push and pull request targeting `main`.

Steps:

1. Check out the repository
2. Install Python 3.12 via `uv`
3. Install CI dependencies (`uv sync --extra ci`)
4. Validate Kestra flow files using `check_flows.py`
5. Run the test suite with `pytest`

The CI pipeline ensures that:

- all flow YAML files parse correctly
- required fields (`id`, `namespace`, `tasks`) are present
- no duplicate flow IDs exist
- no hardcoded credentials appear in flow definitions
- all Python tests pass

This must succeed before merging into `main`.

---

## CD — Deployment (`deploy.yml`)

### Triggers

- **Push to `prod`** — automatic deployment on every merge or push.
- **Manual dispatch** — triggered via the GitHub Actions UI (`workflow_dispatch`).

Deployment is **not** triggered on `main`.

### Runner

The workflow runs on a **self-hosted GitHub Actions runner** installed on the production VM. This allows the workflow to call the Kestra API at `localhost` without exposing it to the internet.

### Deployment sequence

1. GitHub detects a push to `prod` (or a manual trigger)
2. The `deploy` job starts on the self-hosted runner
3. The repository is checked out (`actions/checkout@v4`)
4. Branch, commit, and hostname are logged for traceability
5. `100-scripts_mlops/deploy/deploy_flows.sh` is executed
6. The script validates each flow via the Kestra API, then updates it

If any step fails, the workflow stops immediately.

---

## Deployment script (`deploy_flows.sh`)

Located at `100-scripts_mlops/deploy/deploy_flows.sh`.

The script iterates over all `.yaml` / `.yml` files in the flow directory and, for each flow:

1. **Validates** the flow via `POST /api/v1/main/flows/validate`
2. **Deploys** the flow via `PUT /api/v1/main/flows/{namespace}/{id}`

### Environment variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KESTRA_URL` | Base URL of the Kestra API | `http://localhost:8082` |
| `FLOW_DIR` | Directory containing flow YAML files | `10-flows` |
| `KESTRA_ADMIN_USER` | Kestra API username | _(required)_ |
| `KESTRA_ADMIN_PASS` | Kestra API password | _(required)_ |

Credentials are provided via GitHub repository secrets — they are never hardcoded.

The script uses `set -euo pipefail` so any error (failed validation, network issue, bad response) causes an immediate failure.

---

## Secrets

The following secrets must be configured in the GitHub repository settings:

| Secret | Used by |
|--------|---------|
| `KESTRA_ADMIN_USER` | `deploy.yml` → `deploy_flows.sh` |
| `KESTRA_ADMIN_PASS` | `deploy.yml` → `deploy_flows.sh` |

---

## Development workflow

```
feature branch ──► PR to main ──► CI validates ──► merge to main ──► merge main → prod ──► CD deploys
```

1. Create a feature branch from `main`
2. Develop and test locally (`pytest 130-tests/ -v`)
3. Open a pull request to `main`
4. CI runs validation — all checks must pass
5. Merge to `main`
6. When ready for production, merge `main` into `prod`
7. CD automatically deploys the updated flows to the production Kestra instance
