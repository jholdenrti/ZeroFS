# ZeroFS CSI Driver for Kubernetes

A Container Storage Interface (CSI) driver that enables Kubernetes to use ZeroFS volumes. ZeroFS is a high-performance filesystem that uses S3 as primary storage, accessible via NFS, 9P, and NBD protocols.

This CSI driver leverages the **9P protocol over TCP** to provide efficient, network-based filesystem access to Kubernetes pods.

## Features

- **Path-based Volume Isolation**: Mount specific subdirectories from a shared ZeroFS instance
- **9P Protocol**: Uses the Plan 9 filesystem protocol (9P2000.L) for better POSIX semantics than NFS
- **Multi-node Access**: Supports ReadWriteMany (RWX) access mode
- **Multi-tenancy**: One ZeroFS instance can serve multiple isolated volumes via path parameter
- **Standard CSI Implementation**: Compatible with Kubernetes 1.21+
- **Lightweight**: Minimal resource overhead, privileged containers only in CSI DaemonSet

## Architecture

### Recommended Deployment Pattern

**One ZeroFS instance → Multiple PVs via path parameter:**

```
┌──────────────────────────────────────────────────────┐
│ ZeroFS Instance (zerofs-system namespace)            │
│   → S3://my-bucket/                                  │
│      ├── /app1/data/    ← PV 1 (path: /app1/data)   │
│      ├── /app2/logs/    ← PV 2 (path: /app2/logs)   │
│      └── /shared/       ← PV 3 (path: /shared)      │
└──────────────────────────────────────────────────────┘
                         ↑
                    9P TCP mounts
                         ↓
┌──────────────────────────────────────────────────────┐
│ CSI Node Plugin DaemonSet (kube-system)              │
│   - Privileged pods on each node                     │
│   - Mounts ZeroFS volumes via 9P                     │
│   - Bind-mounts subdirectories to pods               │
└──────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────┐
│ Application Pods (any namespace)                     │
│   - Unprivileged                                     │
│   - See only their scoped subdirectory               │
│   - Multiple pods can share same PV (RWX)            │
└──────────────────────────────────────────────────────┘
```

**Key Points:**
- **ZeroFS Isolation**: Each ZeroFS instance serves one S3 bucket (ZeroFS read-write limitation)
- **Path Isolation**: Multiple PVs reference the same ZeroFS with different `path` parameters
- **Security**: Only CSI DaemonSet runs privileged; application pods are unprivileged
- **Flexibility**: Choose between dedicated ZeroFS per app or shared with path isolation

### CSI Components

#### Controller Plugin (Deployment)
- Handles volume lifecycle operations (create, delete)
- Optional for static provisioning (manual PV creation)
- Runs in `kube-system` namespace

#### Node Plugin (DaemonSet)
- **Privileged pods** that mount ZeroFS volumes using 9P protocol
- One pod per node for mounting operations
- Exposes mounted volumes to unprivileged application pods
- Uses `node-driver-registrar` sidecar for kubelet registration

## Prerequisites

### Kubernetes Cluster
- Kubernetes 1.21 or later
- CSI support enabled (default in modern Kubernetes)

### Node Requirements
- Linux kernel with 9P support (v9fs module)
  ```bash
  # Check if 9p module is available
  modprobe 9p
  lsmod | grep 9p
  ```
- Modern kernels (5.x+) include 9P support by default

### ZeroFS Server
- ZeroFS instance configured with 9P server enabled
- Network accessibility from Kubernetes nodes
- S3-compatible object storage backend

## Installation

### 1. Deploy ZeroFS Server

First, deploy a ZeroFS server instance (or use an existing one):

```bash
# Update deploy/zerofs-server.yaml with your S3 credentials
kubectl apply -f deploy/zerofs-server.yaml
```

This creates:
- ConfigMap with ZeroFS configuration
- StatefulSet running ZeroFS server
- Service exposing 9P on port 5564

### 2. Build and Push CSI Driver Image

```bash
# Build the driver image
cd csi/
docker build -t your-registry/zerofs-csi-driver:v1.0.0 .

# Push to your container registry
docker push your-registry/zerofs-csi-driver:v1.0.0
```

Update the image references in `deploy/controller.yaml` and `deploy/node.yaml`.

### 3. Deploy CSI Driver

```bash
# Install RBAC resources
kubectl apply -f deploy/rbac.yaml

# Deploy controller
kubectl apply -f deploy/controller.yaml

# Deploy node plugin
kubectl apply -f deploy/node.yaml

# Create StorageClass
kubectl apply -f deploy/storageclass.yaml
```

### 4. Verify Installation

```bash
# Check controller pod
kubectl get pods -n kube-system -l app=zerofs-csi-controller

# Check node pods (one per node)
kubectl get pods -n kube-system -l app=zerofs-csi-node

# Verify CSI driver registration
kubectl get csidrivers
# Should show: zerofs.csi.k8s.io
```

## Usage

### Create a PersistentVolumeClaim

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-zerofs-volume
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: zerofs
  resources:
    requests:
      storage: 10Gi
```

```bash
kubectl apply -f examples/pvc.yaml
```

### Use in a Pod

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: app-pod
spec:
  containers:
    - name: app
      image: nginx
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: my-zerofs-volume
```

```bash
kubectl apply -f examples/pod.yaml
```

### Verify Volume Mount

```bash
# Exec into the pod
kubectl exec -it app-pod -- sh

# Check the mount
df -h /data
mount | grep /data

# Write some data
echo "Hello from ZeroFS!" > /data/test.txt
cat /data/test.txt
```

## Configuration

### PersistentVolume Parameters

When creating PVs, you can specify these volumeAttributes:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: myapp-data
spec:
  csi:
    driver: zerofs.csi.k8s.io
    volumeHandle: myapp-data
    volumeAttributes:
      # ZeroFS server address (required)
      # Use full FQDN for cross-namespace access
      server: "zerofs-myapp.zerofs-system.svc.cluster.local"

      # 9P port (optional, defaults to 5564)
      # port: "5564"

      # Subdirectory path to mount (optional)
      # If specified, only this subdirectory is visible in pods
      # If omitted, entire volume root is mounted
      # path: "/app1/data"
```

**Path Parameter Examples:**

```yaml
# Mount entire volume (no path specified)
volumeAttributes:
  server: "zerofs-shared.zerofs-system.svc.cluster.local"
  # Pods see: / (root of ZeroFS volume)

# Mount specific subdirectory
volumeAttributes:
  server: "zerofs-shared.zerofs-system.svc.cluster.local"
  path: "/app1/data"
  # Pods see: /app1/data only (isolated from other paths)

# Mount nested subdirectory
volumeAttributes:
  server: "zerofs-shared.zerofs-system.svc.cluster.local"
  path: "/projects/2024/app1"
  # Pods see: /projects/2024/app1 only
```

**Use Cases for Path Parameter:**
- **Multi-tenancy**: One ZeroFS instance, multiple apps with isolated subdirectories
- **Access control**: Limit pod access to specific subdirectories
- **Organization**: Structure data hierarchically while sharing one ZeroFS backend
- **Cost optimization**: Fewer ZeroFS instances to manage

See `examples/pv-with-path.yaml` for complete examples.

### StorageClass Parameters (For Dynamic Provisioning)

Edit `deploy/storageclass.yaml` to configure:

```yaml
parameters:
  # ZeroFS server address (required)
  server: "zerofs-service.default.svc.cluster.local"

  # 9P port (optional, default: 5564)
  port: "5564"

  # Base directory for volumes (optional, default: /volumes)
  subdir: "/volumes"

  # Temporary mount point for controller (optional)
  mountPoint: "/tmp/zerofs-csi-mount"

mountOptions:
  # 9P transport options
  - trans=tcp
  - version=9p2000.L
  - cache=mmap          # Caching policy: none, loose, fscache, mmap
  - access=user
```

**Note:** The current implementation is optimized for static PV provisioning with manual ZeroFS deployment management.

### Mount Options

The following 9P mount options are supported:

- `trans=tcp` - Transport protocol (always TCP for this driver)
- `version=9p2000.L` - Use Linux extensions for better POSIX semantics
- `cache=` - Caching policy:
  - `none` - No caching (slowest, most consistent)
  - `loose` - Loose caching (faster, less consistent)
  - `mmap` - Memory-mapped caching (recommended)
  - `fscache` - Use FS-Cache if available
- `access=user` - Access check level
- `msize=` - Maximum 9P packet size (default: 8192)

## Troubleshooting

### Volume Not Mounting

**Error: "unknown filesystem type '9p'"**

The kernel doesn't have 9P support. Load the module:

```bash
# On each node:
sudo modprobe 9p
sudo modprobe 9pnet
sudo modprobe 9pnet_virtio

# Make permanent (add to /etc/modules)
echo "9p" | sudo tee -a /etc/modules
echo "9pnet" | sudo tee -a /etc/modules
```

### Connection Refused

**Error: "connection refused to ZeroFS server"**

Check connectivity:

```bash
# From a node, test 9P port
nc -zv zerofs-service.default.svc.cluster.local 5564

# Check ZeroFS server logs
kubectl logs -n default zerofs-server-0
```

### Pod Stuck in ContainerCreating

```bash
# Check node plugin logs
kubectl logs -n kube-system -l app=zerofs-csi-node -c zerofs

# Check events
kubectl describe pod <pod-name>
```

### Volume Not Deleted

Volumes are created as subdirectories in ZeroFS. The current implementation logs deletion requests but doesn't remove directories. To enable actual deletion, update the `DeleteVolume` function in `pkg/driver/controller.go`.

## Development

### Project Structure

```
csi/
├── cmd/zerofsplugin/     # Main entry point
├── pkg/
│   ├── driver/           # CSI service implementations
│   ├── ninep/            # 9P client helpers
│   └── util/             # Mount utilities
├── deploy/               # Kubernetes manifests
├── examples/             # Usage examples
└── Dockerfile            # Container image
```

### Building

```bash
# Build locally
cd csi/
go mod download
go build -o zerofsplugin cmd/zerofsplugin/main.go

# Run tests
go test ./...

# Build Docker image
docker build -t zerofs-csi-driver:dev .
```

### Testing with kind

```bash
# Create kind cluster
kind create cluster --name zerofs-test

# Load image into kind
kind load docker-image zerofs-csi-driver:dev --name zerofs-test

# Deploy CSI driver
kubectl apply -f deploy/

# Test volume provisioning
kubectl apply -f examples/pvc.yaml
kubectl apply -f examples/pod.yaml
```

## Limitations

- **9P Kernel Module Required**: All nodes must have v9fs support
- **TCP Only**: This implementation uses TCP transport only (Unix sockets require ZeroFS on each node)
- **No Snapshots**: Volume snapshots not currently supported
- **No Resize**: Volume expansion not currently supported
- **No Volume Metrics**: NodeGetVolumeStats not implemented

## Roadmap

- [ ] Implement actual volume deletion in `DeleteVolume`
- [ ] Add volume metrics support (`NodeGetVolumeStats`)
- [ ] Support Unix socket transport option
- [ ] Add volume expansion capability
- [ ] Implement snapshot support
- [ ] Add comprehensive test suite
- [ ] Performance benchmarks and optimization

## References

- [Kubernetes CSI Documentation](https://kubernetes-csi.github.io/docs/)
- [ZeroFS Documentation](../README.md)
- [9P Protocol Specification](http://9p.cat-v.org/documentation/)
- [Azure Files CSI Driver](https://github.com/kubernetes-sigs/azurefile-csi-driver) (reference implementation)

## License

Same license as ZeroFS project.
