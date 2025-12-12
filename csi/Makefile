# ZeroFS CSI Driver Makefile

# Configuration
REGISTRY ?= docker.io/youruser
IMAGE_NAME = zerofs-csi-driver
VERSION ?= v1.0.0
IMAGE = $(REGISTRY)/$(IMAGE_NAME):$(VERSION)

# Go parameters
GOCMD = go
GOBUILD = $(GOCMD) build
GOCLEAN = $(GOCMD) clean
GOTEST = $(GOCMD) test
GOGET = $(GOCMD) get
GOMOD = $(GOCMD) mod

# Binary name
BINARY_NAME = zerofsplugin
BINARY_PATH = ./bin/$(BINARY_NAME)

# Build parameters
LDFLAGS = -s -w

.PHONY: all build clean test docker-build docker-push deploy undeploy help

all: test build

## build: Build the CSI driver binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) \
		-ldflags="$(LDFLAGS)" \
		-o $(BINARY_PATH) \
		./cmd/zerofsplugin/main.go
	@echo "Binary built: $(BINARY_PATH)"

## build-local: Build for local OS (development)
build-local:
	@echo "Building $(BINARY_NAME) for local OS..."
	@mkdir -p bin
	$(GOBUILD) -o $(BINARY_PATH) ./cmd/zerofsplugin/main.go

## test: Run tests
test:
	@echo "Running tests..."
	$(GOTEST) -v -race -coverprofile=coverage.out ./...

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf bin/
	rm -f coverage.out

## docker-build: Build Docker image
docker-build:
	@echo "Building Docker image: $(IMAGE)"
	docker build -t $(IMAGE) .
	docker tag $(IMAGE) $(REGISTRY)/$(IMAGE_NAME):latest

## docker-push: Push Docker image to registry
docker-push:
	@echo "Pushing Docker image: $(IMAGE)"
	docker push $(IMAGE)
	docker push $(REGISTRY)/$(IMAGE_NAME):latest

## docker-build-push: Build and push Docker image
docker-build-push: docker-build docker-push

## deploy: Deploy CSI driver to Kubernetes
deploy:
	@echo "Deploying ZeroFS CSI driver..."
	kubectl apply -f deploy/rbac.yaml
	kubectl apply -f deploy/csidriver.yaml
	kubectl apply -f deploy/controller.yaml
	kubectl apply -f deploy/node.yaml
	kubectl apply -f deploy/storageclass.yaml
	@echo "Waiting for controller to be ready..."
	kubectl wait --for=condition=available --timeout=60s \
		deployment/zerofs-csi-controller -n kube-system
	@echo "Deployment complete!"

## undeploy: Remove CSI driver from Kubernetes
undeploy:
	@echo "Removing ZeroFS CSI driver..."
	kubectl delete -f deploy/storageclass.yaml --ignore-not-found
	kubectl delete -f deploy/node.yaml --ignore-not-found
	kubectl delete -f deploy/controller.yaml --ignore-not-found
	kubectl delete -f deploy/csidriver.yaml --ignore-not-found
	kubectl delete -f deploy/rbac.yaml --ignore-not-found

## deploy-server: Deploy example ZeroFS server
deploy-server:
	@echo "Deploying ZeroFS server..."
	kubectl apply -f deploy/zerofs-server.yaml

## undeploy-server: Remove ZeroFS server
undeploy-server:
	@echo "Removing ZeroFS server..."
	kubectl delete -f deploy/zerofs-server.yaml --ignore-not-found

## example: Deploy example PVC and Pod
example:
	@echo "Deploying example resources..."
	kubectl apply -f examples/pvc.yaml
	kubectl apply -f examples/pod.yaml
	@echo "Waiting for pod to be ready..."
	kubectl wait --for=condition=ready --timeout=60s pod/zerofs-test-pod
	@echo "Example deployed! Check with: kubectl exec -it zerofs-test-pod -- sh"

## example-clean: Remove example resources
example-clean:
	@echo "Removing example resources..."
	kubectl delete -f examples/pod.yaml --ignore-not-found
	kubectl delete -f examples/pvc.yaml --ignore-not-found

## logs-controller: Show controller logs
logs-controller:
	kubectl logs -n kube-system -l app=zerofs-csi-controller -c zerofs --tail=100 -f

## logs-node: Show node plugin logs
logs-node:
	kubectl logs -n kube-system -l app=zerofs-csi-node -c zerofs --tail=100 -f

## status: Show CSI driver status
status:
	@echo "=== Controller Status ==="
	kubectl get pods -n kube-system -l app=zerofs-csi-controller
	@echo ""
	@echo "=== Node Plugin Status ==="
	kubectl get pods -n kube-system -l app=zerofs-csi-node
	@echo ""
	@echo "=== CSI Driver ==="
	kubectl get csidriver zerofs.csi.k8s.io 2>/dev/null || echo "CSI driver not registered"
	@echo ""
	@echo "=== Storage Classes ==="
	kubectl get storageclass zerofs 2>/dev/null || echo "StorageClass not found"

## mod-download: Download Go modules
mod-download:
	$(GOMOD) download

## mod-tidy: Tidy Go modules
mod-tidy:
	$(GOMOD) tidy

## help: Show this help message
help:
	@echo "ZeroFS CSI Driver Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'
