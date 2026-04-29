# Variables
CARGO            = cargo
FLAGS            = --all-features --workspace
REGISTRY        ?= quay.io
REPOSITORY      ?= the-conn
IMAGE_NAME      ?= jefferies
TAG             ?= latest
FULL_IMAGE_NAME := $(REGISTRY)/$(REPOSITORY)/$(IMAGE_NAME):$(TAG)

CONTAINER_ENGINE := $(shell which podman 2>/dev/null || which docker)

.PHONY: all build test lint fmt image push clean help run

all: fmt lint test build

## build: Compile the backend binary locally
build:
	@echo "Compiling $(IMAGE_NAME) locally..."
	$(CARGO) build --release $(FLAGS)

## test: Run all unit and integration tests
test:
	@echo "Running all unit tests..."
	$(CARGO) test $(FLAGS)

## lint: Run clippy for static analysis
lint:
	@echo "Running clippy..."
	$(CARGO) clippy $(FLAGS) -- -D warnings

## fmt: Format code using nightly rustfmt
fmt:
	@echo "Formatting code..."
	$(CARGO) +nightly fmt

## image: Build the container image
image:
	@echo "Building $(FULL_IMAGE_NAME) using $(CONTAINER_ENGINE)..."
	$(CONTAINER_ENGINE) build -t $(IMAGE_NAME):$(TAG) -t $(FULL_IMAGE_NAME) .

## push: Push the image to the remote registry
push:
	@echo "Pushing $(FULL_IMAGE_NAME) to registry..."
	$(CONTAINER_ENGINE) push $(FULL_IMAGE_NAME)

## run: Run the backend locally
run:
	$(CARGO) run $(FLAGS)

## clean: Remove build artifacts and local images
clean:
	@echo "Cleaning artifacts..."
	$(CARGO) clean
	$(CONTAINER_ENGINE) rmi $(FULL_IMAGE_NAME) || true

## help: Show this help message
help:
	@echo "Usage: make [target] [VARIABLES...]"
	@echo ""
	@echo "Targets:"
	@grep -E '^##' $(MAKEFILE_LIST) | sed -e 's/## //' | column -t -s ':'
