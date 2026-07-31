# ────────────────────────────────────────────────────────────────────
# Hermes Proxy Relay — Makefile
# ────────────────────────────────────────────────────────────────────

.DEFAULT_GOAL := help

VENV ?= .venv
PYTHON ?= python3

.PHONY: help install run test lint clean smoke coverage

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -r requirements.txt

venv: ## Create virtual environment and install deps
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install -r requirements.txt
	@echo "Activate: source $(VENV)/bin/activate"

run: ## Run the relay (set PROXY_LIST and UPSTREAM vars first)
	$(PYTHON) relay/relay.py

test: ## Run all tests
	$(PYTHON) -m pytest tests/ -v --tb=short

test-quick: ## Run quick tests (no endpoint tests)
	$(PYTHON) -m pytest tests/test_cooldown_pool.py tests/test_relay_utils.py -v --tb=short

smoke: ## Run end-to-end smoke test (starts relay on :4997)
	./scripts/smoke_test.sh

coverage: ## Run tests with coverage report
	$(PYTHON) -m pytest tests/ -q --cov=relay --cov-report=term-missing

lint: ## Run ruff linter
	ruff check . || true

clean: ## Clean cache files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf *.egg-info dist build

version: ## Show relay version
	$(PYTHON) relay/relay.py --version

docker-build: ## Build Docker image
	docker build -t hermes-proxy-relay .

docker-run: ## Run Docker container
	docker run -d -p 4002:4002 \
		-v ~/.hermes/proxy-relay:/data/config \
		-e PROXY_LIST=/data/config/proxies.txt \
		-e RELAY_CONFIG=/data/config/config.json \
		hermes-proxy-relay
