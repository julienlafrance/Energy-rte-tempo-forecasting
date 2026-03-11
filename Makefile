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

check-flows: ## Validate Kestra flows
	uv run python 100-scripts_mlops/ci/check_flows.py

check: check-flows test pre-commit ## Run all checks (flows + tests + pre-commit)

.PHONY: help install pre-commit fix test check-flows check
