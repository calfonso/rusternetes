.PHONY: help build test clean dev-up dev-down dev-logs dev-clean install-deps fmt check clippy run-api-server run-scheduler run-controller run-kubelet run-kube-proxy kubectl build-images push-images

# Detect container runtime (podman or docker)
CONTAINER_RUNTIME := $(shell command -v podman 2> /dev/null || command -v docker 2> /dev/null)
COMPOSE_CMD := $(shell command -v podman-compose 2> /dev/null || command -v docker-compose 2> /dev/null || (docker compose version > /dev/null 2>&1 && echo "docker compose"))

# Select the right compose file for the runtime
ifeq ($(findstring podman,$(COMPOSE_CMD)),podman)
COMPOSE_FILE ?= compose.yml
else
COMPOSE_FILE ?= docker-compose.yml
endif
COMPOSE := $(COMPOSE_CMD) -f $(COMPOSE_FILE)

# Container image configuration
IMAGE_PREFIX ?= rusternetes
IMAGE_TAG ?= latest

# Colors for output
BOLD := \033[1m
GREEN := \033[0;32m
YELLOW := \033[1;33m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BOLD)Rusternetes Development Makefile$(NC)"
	@echo ""
	@echo "$(BOLD)Available targets:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

# Rust Development
build: ## Build all Rust binaries in release mode
	@echo "$(GREEN)Building all binaries...$(NC)"
	cargo build --release

build-dev: ## Build all Rust binaries in debug mode
	@echo "$(GREEN)Building all binaries (debug mode)...$(NC)"
	cargo build

test: ## Run all tests
	@echo "$(GREEN)Running tests...$(NC)"
	cargo test

test-verbose: ## Run all tests with verbose output
	@echo "$(GREEN)Running tests (verbose)...$(NC)"
	cargo test -- --nocapture

check: ## Run cargo check
	@echo "$(GREEN)Running cargo check...$(NC)"
	cargo check --all-targets

clippy: ## Run clippy linter
	@echo "$(GREEN)Running clippy...$(NC)"
	cargo clippy --all-targets --all-features -- -D warnings

fmt: ## Format code with rustfmt
	@echo "$(GREEN)Formatting code...$(NC)"
	cargo fmt --all

fmt-check: ## Check code formatting without making changes
	@echo "$(GREEN)Checking code formatting...$(NC)"
	cargo fmt --all -- --check

clean: ## Clean build artifacts
	@echo "$(GREEN)Cleaning build artifacts...$(NC)"
	cargo clean

# Container Development
build-images: ## Build all container images
	@echo "$(GREEN)Building all container images...$(NC)"
	$(COMPOSE) build

build-image-%: ## Build a specific component image (e.g., make build-image-api-server)
	@echo "$(GREEN)Building $* image...$(NC)"
	$(CONTAINER_RUNTIME) build -f Dockerfile.$* -t $(IMAGE_PREFIX)/$*:$(IMAGE_TAG) .

dev-up: ## Start the development cluster
	@if [ -z "$$KUBELET_VOLUMES_PATH" ]; then \
		echo "$(YELLOW)ERROR: KUBELET_VOLUMES_PATH environment variable is not set!$(NC)"; \
		echo ""; \
		echo "You must set this to an absolute path before starting the cluster."; \
		echo ""; \
		echo "Example:"; \
		echo "  export KUBELET_VOLUMES_PATH=\$$(pwd)/.rusternetes/volumes"; \
		echo "  make dev-up"; \
		echo ""; \
		echo "Or in one command:"; \
		echo "  KUBELET_VOLUMES_PATH=\$$(pwd)/.rusternetes/volumes make dev-up"; \
		exit 1; \
	fi
	@echo "$(GREEN)Starting development cluster...$(NC)"
	@echo "Using volume path: $$KUBELET_VOLUMES_PATH"
	$(COMPOSE) up -d
	@echo ""
	@echo "$(BOLD)Cluster started!$(NC)"
	@echo "API Server: http://localhost:6443"
	@echo "etcd: http://localhost:2379"

dev-down: ## Stop the development cluster
	@echo "$(GREEN)Stopping development cluster...$(NC)"
	$(COMPOSE) down

dev-restart: ## Restart the development cluster
	@echo "$(GREEN)Restarting development cluster...$(NC)"
	$(COMPOSE) restart

dev-logs: ## View logs from all services
	$(COMPOSE) logs -f

dev-logs-%: ## View logs from a specific service (e.g., make dev-logs-api-server)
	$(COMPOSE) logs -f $*

dev-clean: ## Clean up all containers, volumes, and networks
	@echo "$(YELLOW)WARNING: This will remove all containers, volumes, and networks$(NC)"
	@read -p "Continue? [y/N]: " confirm; \
	if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
		$(COMPOSE) down -v; \
		echo "$(GREEN)Cleanup complete!$(NC)"; \
	else \
		echo "Cleanup cancelled."; \
	fi

dev-ps: ## Show running containers
	$(COMPOSE) ps

dev-exec-%: ## Execute a shell in a running container (e.g., make dev-exec-api-server)
	$(COMPOSE) exec $* /bin/sh

# Local Binary Execution
run-api-server: ## Run API server locally
	cargo run --bin api-server -- --bind-address 0.0.0.0:6443 --etcd-servers http://localhost:2379

run-scheduler: ## Run scheduler locally
	cargo run --bin scheduler -- --etcd-servers http://localhost:2379

run-controller: ## Run controller manager locally
	cargo run --bin controller-manager -- --etcd-servers http://localhost:2379

run-kubelet: ## Run kubelet locally
	cargo run --bin kubelet -- --node-name node-1 --etcd-servers http://localhost:2379

run-kube-proxy: ## Run kube-proxy locally
	cargo run --bin kube-proxy -- --node-name node-1

# kubectl Commands
kubectl-get-pods: ## List all pods
	cargo run --bin kubectl -- --server http://localhost:6443 get pods

kubectl-get-deployments: ## List all deployments
	cargo run --bin kubectl -- --server http://localhost:6443 get deployments

kubectl-get-services: ## List all services
	cargo run --bin kubectl -- --server http://localhost:6443 get services

kubectl-get-namespaces: ## List all namespaces
	cargo run --bin kubectl -- --server http://localhost:6443 get namespaces

kubectl-create-example-pod: ## Create example pod
	cargo run --bin kubectl -- --server http://localhost:6443 create -f examples/pod.yaml

kubectl-create-example-deployment: ## Create example deployment
	cargo run --bin kubectl -- --server http://localhost:6443 create -f examples/deployment.yaml

# Dependencies
install-deps: ## Install required system dependencies (macOS/Linux)
	@echo "$(GREEN)Installing dependencies...$(NC)"
	@if [ "$$(uname)" = "Darwin" ]; then \
		echo "$(GREEN)Detecting Homebrew...$(NC)"; \
		if [ -x /opt/homebrew/bin/brew ]; then \
			BREW=/opt/homebrew/bin/brew; \
		elif [ -x /usr/local/bin/brew ]; then \
			BREW=/usr/local/bin/brew; \
		else \
			echo "$(YELLOW)Homebrew not found. Install it from https://brew.sh$(NC)"; \
			exit 1; \
		fi; \
		echo "Using $$BREW"; \
		$$BREW install podman podman-compose docker-compose; \
		echo "$(GREEN)Dependencies installed!$(NC)"; \
		echo ""; \
		echo "$(BOLD)Next steps:$(NC)"; \
		echo "  1. podman machine init"; \
		echo "  2. podman machine set --rootful"; \
		echo "  3. podman machine start"; \
	elif [ -f /etc/debian_version ]; then \
		echo "$(GREEN)Installing via apt...$(NC)"; \
		sudo apt-get update && sudo apt-get install -y podman podman-compose docker-compose; \
		echo "$(GREEN)Dependencies installed!$(NC)"; \
	elif [ -f /etc/redhat-release ]; then \
		echo "$(GREEN)Installing via dnf...$(NC)"; \
		sudo dnf install -y podman podman-compose docker-compose; \
		echo "$(GREEN)Dependencies installed!$(NC)"; \
	else \
		echo "$(YELLOW)Unsupported OS. Please install podman, podman-compose, and docker-compose manually.$(NC)"; \
	fi

# Full Development Workflow
dev-full: ## Build images and start development cluster
	@if [ -z "$$KUBELET_VOLUMES_PATH" ]; then \
		echo "$(YELLOW)ERROR: KUBELET_VOLUMES_PATH environment variable is not set!$(NC)"; \
		echo ""; \
		echo "You must set this to an absolute path before starting the cluster."; \
		echo ""; \
		echo "Example:"; \
		echo "  export KUBELET_VOLUMES_PATH=\$$(pwd)/.rusternetes/volumes"; \
		echo "  make dev-full"; \
		exit 1; \
	fi
	@$(MAKE) build-images
	@$(MAKE) dev-up
	@echo ""
	@echo "$(BOLD)$(GREEN)Development environment is ready!$(NC)"
	@echo ""
	@echo "Next steps:"
	@echo "  - View logs: make dev-logs"
	@echo "  - List pods: make kubectl-get-pods"
	@echo "  - Stop cluster: make dev-down"

# Quick start
quick-start: ## Interactive setup using dev-setup.sh script
	./scripts/dev-setup.sh

# Pre-commit checks
pre-commit: fmt clippy test ## Run pre-commit checks (format, lint, test)
	@echo "$(BOLD)$(GREEN)All pre-commit checks passed!$(NC)"

# CI/CD simulation
ci: fmt-check clippy test build ## Run CI checks locally
	@echo "$(BOLD)$(GREEN)All CI checks passed!$(NC)"
