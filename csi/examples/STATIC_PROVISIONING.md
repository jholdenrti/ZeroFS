# Static Provisioning with Automatic ZeroFS Deployment

## Overview

The ZeroFS CSI driver automatically creates and manages ZeroFS deployments based on the storage URL specified in PersistentVolumes.

### Key Concepts

1. **PV Owns Storage Backend**: Each PV defines its storage backend (S3 bucket, Azure container, etc.)
2. **Automatic Deduplication**: Multiple PVs with the same URL share one ZeroFS deployment
3. **Path Isolation**: Use `path` parameter to isolate different apps using the same ZeroFS instance
4. **Automatic Cleanup**: When the last PV is deleted, the ZeroFS deployment is automatically removed

## How It Works

```
┌─────────────────────────────────────────────────────────┐
│ User creates PV with storage URL                         │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│ CSI Controller                                           │
│  1. Hash URL + account + region + endpoint              │
│  2. Check if Deployment exists for this hash             │
│  3. If not, create Deployment + Service                  │
│  4. Return Service address in Volume Context             │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│ CSI Node Plugin mounts volume to pod                     │
└─────────────────────────────────────────────────────────┘
```

## Volume Attributes

### Required

- `url`: Storage backend URL
  - S3: `s3://bucket-name/prefix/`
  - Azure: `az://container-name/prefix/`
  - GCS: `gs://bucket-name/prefix/`

### Optional (S3)

- `region`: AWS region (e.g., `us-east-1`)
- `endpoint`: Custom S3 endpoint (for MinIO, etc.)

### Optional (Azure)

- `storageAccountName`: Azure storage account name

### Optional (All)

- `path`: Subdirectory to mount (for isolation)
- `credentialsSecret`: Secret name in `zerofs-system` namespace
- `encryptionSecret`: Secret name for encryption password

## Examples

### Dedicated ZeroFS per Application

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: app1-storage
spec:
  csi:
    driver: zerofs.csi.k8s.io
    volumeHandle: app1-storage
    volumeAttributes:
      url: "s3://app1-bucket/"
      region: "us-east-1"
      credentialsSecret: "s3-credentials"
```

**Result**: Creates `zerofs-abc123def` deployment for this bucket

### Shared ZeroFS with Path Isolation

```yaml
# PV 1
apiVersion: v1
kind: PersistentVolume
metadata:
  name: team-alpha
spec:
  csi:
    volumeAttributes:
      url: "s3://shared-bucket/"
      path: "/teams/alpha"
---
# PV 2 (reuses same ZeroFS!)
apiVersion: v1
kind: PersistentVolume
metadata:
  name: team-beta
spec:
  csi:
    volumeAttributes:
      url: "s3://shared-bucket/"  # Same URL
      path: "/teams/beta"          # Different path
```

**Result**: Both PVs share deployment `zerofs-xyz789abc`

## Uniqueness Hash

ZeroFS deployments are uniquely identified by a hash of:
- `url` (required)
- `storageAccountName` (if provided)
- `region` (if provided)
- `endpoint` (if provided)

**Example hashes:**
- `s3://bucket/` → `zerofs-a1b2c3d4`
- `s3://bucket/` + `region=us-east-1` → `zerofs-e5f6g7h8`
- `s3://bucket/` + `endpoint=minio:9000` → `zerofs-i9j0k1l2`

## Lifecycle

### PV Creation
1. CSI controller hashes storage parameters
2. Checks if ZeroFS deployment exists
3. If not, creates Deployment + Service in `zerofs-system` namespace
4. Returns Service FQDN in volume context

### PV Deletion
1. CSI controller finds deployment name from PV
2. Counts other PVs referencing same deployment
3. If last reference, deletes Deployment + Service
4. If `reclaimPolicy: Delete`, optionally wipes data

## Secrets

Create secrets in `zerofs-system` namespace:

### S3 Credentials
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: s3-credentials
  namespace: zerofs-system
stringData:
  access-key-id: "AKIAIOSFODNN7EXAMPLE"
  secret-access-key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

### Azure Credentials
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: azure-credentials
  namespace: zerofs-system
stringData:
  azure-storage-key: "your-azure-storage-account-key"
```

### Encryption Password
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: zerofs-encryption
  namespace: zerofs-system
stringData:
  password: "your-strong-encryption-password"
```

## Best Practices

1. **Use Secrets**: Never hardcode credentials in PV specs
2. **Path Isolation**: Use paths for multi-tenant scenarios
3. **ReclaimPolicy**: Use `Retain` for production data
4. **Resource Limits**: ZeroFS deployments have default limits (2Gi RAM, 1 CPU)
5. **Monitoring**: Watch ZeroFS deployment pods in `zerofs-system` namespace

## Troubleshooting

### Check ZeroFS Deployments
```bash
kubectl get deployments -n zerofs-system -l app=zerofs
```

### Check Deployment Logs
```bash
kubectl logs -n zerofs-system deployment/zerofs-abc123def
```

### Check PV References
```bash
kubectl get pv -o json | jq '.items[] | select(.spec.csi.volumeAttributes.deployment == "zerofs-abc123def") | .metadata.name'
```

### Manual Cleanup
If deployments become orphaned:
```bash
# List all ZeroFS deployments
kubectl get deployments -n zerofs-system -l app=zerofs

# Delete orphaned deployment
kubectl delete deployment -n zerofs-system zerofs-abc123def
kubectl delete service -n zerofs-system zerofs-abc123def
```
