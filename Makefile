.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dev dependencies and pre-commit hooks
	uv sync --extra dev
	uv run pre-commit install

pre-commit: ## Run all pre-commit checks
	uv run pre-commit run --all-files

fix: ## Auto-fix formatting (trailing whitespace, end-of-file)
	uv run pre-commit run trailing-whitespace --all-files || true
	uv run pre-commit run end-of-file-fixer --all-files || true

test: ## Run pytest
	uv run pytest 130-tests/ -v

test-yaml: ## Lint YAML flows (same check as CI)
	uv run yamllint -d '{extends: default, rules: {line-length: {max: 200}, truthy: disable, comments-indentation: disable}}' 10-flows/prod/

check-flows: ## Validate Kestra flows
	uv run python 95-ci-cd/ci/check_flows.py

check: test-yaml check-flows test pre-commit ## Run all checks (yaml + flows + tests + pre-commit)

.PHONY: help install pre-commit fix test test-yaml check-flows check
