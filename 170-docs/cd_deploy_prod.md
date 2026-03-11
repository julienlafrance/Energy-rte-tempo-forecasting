# Continuous Deployment — Kestra Flows

## Triggers

Deployment runs in two cases:

- **Push to `prod`** — automatic deployment on every merge/push to the production branch.
- **Manual dispatch** — triggered via the GitHub Actions UI (`workflow_dispatch`).

Deployment is **not** triggered on `main`.

## Runner

The workflow executes on a **self-hosted GitHub Actions runner** installed on the production VM. Kestra is accessible locally at `http://localhost:8082`.

## What it does

1. Checks out the repository.
2. Logs branch, commit SHA, and runner hostname for traceability.
3. Runs `100-scripts_mlops/deploy/deploy_flows.sh`, which validates and updates Kestra flows via the API.

## Secrets

The following repository secrets must be configured in GitHub:

| Secret               | Description                  |
|----------------------|------------------------------|
| `KESTRA_ADMIN_USER`  | Kestra API admin username    |
| `KESTRA_ADMIN_PASS`  | Kestra API admin password    |
