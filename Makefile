# Variables
CARGO            = cargo
FLAGS            = --all-features --workspace
REGISTRY        ?= quay.io
REPOSITORY      ?= the-conn
IMAGE_NAME      ?= jefferies
TAG             ?= latest
FULL_IMAGE_NAME := $(REGISTRY)/$(REPOSITORY)/$(IMAGE_NAME):$(TAG)

CONTAINER_ENGINE := $(shell which podman 2>/dev/null || which docker)

TEST_IMAGE_NAME = $(IMAGE_NAME)-test
FULL_TEST_IMAGE_NAME = $(REGISTRY)/$(REPOSITORY)/$(TEST_IMAGE_NAME):$(TAG)
TEST_CONTAINERFILE = Containerfile-test

# Added all new targets to .PHONY to ensure they always run
.PHONY: all build test lint fmt fmt-check image push image-test push-test clean clean-test help run ci update rollout

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

## fmt-check: Check if fmt is correct
fmt-check:
	@echo "Checking code format..."
	$(CARGO) +nightly fmt --check

## run: Run the backend locally
run:
	$(CARGO) run $(FLAGS)

## ci: Run the ci checks
ci: fmt-check build lint test

## image: Build the production container image
image:
	@echo "Building $(FULL_IMAGE_NAME) using $(CONTAINER_ENGINE)..."
	$(CONTAINER_ENGINE) build -t $(IMAGE_NAME):$(TAG) -t $(FULL_IMAGE_NAME) .

## push: Push the production image to the remote registry
push:
	@echo "Pushing $(FULL_IMAGE_NAME) to registry..."
	$(CONTAINER_ENGINE) push $(FULL_IMAGE_NAME)

## rollout: Update the deployment on openshift
rollout:
	oc -n the-conn rollout restart deployment jefferies

## update: Builds the image from source, pushes, and rolls out the deployment
update: image push rollout

## image-test: Build the test container image using $(TEST_CONTAINERFILE)
image-test:
	@echo "Building test image: $(FULL_TEST_IMAGE_NAME)..."
	$(CONTAINER_ENGINE) build -f $(TEST_CONTAINERFILE) \
		-t $(TEST_IMAGE_NAME):$(TAG) \
		-t $(FULL_TEST_IMAGE_NAME) .

## push-test: Push the test image to the remote registry
push-test:
	@echo "Pushing test image: $(FULL_TEST_IMAGE_NAME)..."
	$(CONTAINER_ENGINE) push $(FULL_TEST_IMAGE_NAME)

## clean: Remove build artifacts and local images
clean: clean-test
	@echo "Cleaning artifacts..."
	$(CARGO) clean
	$(CONTAINER_ENGINE) rmi $(FULL_IMAGE_NAME) || true

## clean-test: Remove local test images
clean-test:
	@echo "Cleaning test images..."
	$(CONTAINER_ENGINE) rmi $(FULL_TEST_IMAGE_NAME) $(TEST_IMAGE_NAME):$(TAG) || true

## help: Show this help message
help:
	@echo "Usage: make [target] [VARIABLES...]"
	@echo ""
	@echo "Targets:"
	@grep -E '^##' $(MAKEFILE_LIST) | sed -e 's/## //' | column -t -s ':'
