package driver

import (
	"context"
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// ControllerServer implements the CSI Controller service
type ControllerServer struct {
	driver    *Driver
	clientset *kubernetes.Clientset
	csi.UnimplementedControllerServer
}

// NewControllerServer creates a new Controller server
func NewControllerServer(driver *Driver) csi.ControllerServer {
	// Create in-cluster Kubernetes client
	config, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("Failed to create in-cluster config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	return &ControllerServer{
		driver:    driver,
		clientset: clientset,
	}
}

// CreateVolume creates a new volume (creates/reuses ZeroFS deployment)
func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	volumeName := req.GetName()
	if volumeName == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume name must be provided")
	}

	capacityBytes := req.GetCapacityRange().GetRequiredBytes()
	klog.Infof("CreateVolume: name=%s capacity=%d", volumeName, capacityBytes)

	// Extract storage configuration from parameters
	params := req.GetParameters()
	storageConfig, err := getStorageConfigFromParams(params)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid storage configuration: %v", err)
	}

	klog.Infof("Storage config: URL=%s, Account=%s, Region=%s, Endpoint=%s",
		storageConfig.URL, storageConfig.StorageAccountName, storageConfig.Region, storageConfig.Endpoint)

	// Get or create ZeroFS deployment for this storage configuration
	deploymentName, err := cs.getOrCreateZeroFSDeployment(ctx, cs.clientset, storageConfig)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to ensure ZeroFS deployment: %v", err)
	}

	// Build volume context with server address and optional path
	volumeContext := map[string]string{
		"server": fmt.Sprintf("%s.%s.svc.cluster.local", deploymentName, zerofsNamespace),
	}

	// Add path if specified (for subdirectory isolation)
	if path := params["path"]; path != "" {
		volumeContext["path"] = path
	}

	// Store deployment name in volume context for tracking
	volumeContext["deployment"] = deploymentName

	klog.Infof("Successfully created/reused volume %s with deployment %s", volumeName, deploymentName)

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeName,
			CapacityBytes: capacityBytes,
			VolumeContext: volumeContext,
		},
	}, nil
}

// DeleteVolume deletes a volume (removes ZeroFS deployment if last reference)
func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID must be provided")
	}

	klog.Infof("DeleteVolume: volumeID=%s", volumeID)

	// Get the PV to find the deployment name
	pv, err := cs.clientset.CoreV1().PersistentVolumes().Get(ctx, volumeID, metav1.GetOptions{})
	if err != nil {
		klog.Warningf("Failed to get PV %s: %v (may already be deleted)", volumeID, err)
		return &csi.DeleteVolumeResponse{}, nil
	}

	// Extract deployment name from annotations or volume context
	deploymentName := pv.Annotations[annotationDeployment]
	if deploymentName == "" && pv.Spec.CSI != nil {
		deploymentName = pv.Spec.CSI.VolumeAttributes["deployment"]
	}

	if deploymentName == "" {
		klog.Warningf("No deployment name found for volume %s, skipping cleanup", volumeID)
		return &csi.DeleteVolumeResponse{}, nil
	}

	// Count how many PVs still reference this deployment
	refCount, err := cs.countPVsReferencingDeployment(ctx, cs.clientset, deploymentName)
	if err != nil {
		klog.Errorf("Failed to count PV references: %v", err)
		// Don't fail deletion - continue anyway
		refCount = 1 // Assume at least this PV
	}

	// Subtract this PV since it's being deleted
	refCount--

	klog.Infof("Deployment %s has %d remaining PV reference(s) after deleting %s",
		deploymentName, refCount, volumeID)

	if refCount <= 0 {
		// This was the last PV - safe to delete ZeroFS deployment
		klog.Infof("Deleting ZeroFS deployment %s (no more references)", deploymentName)

		// Optional: Wipe data based on reclaim policy
		if pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimDelete {
			klog.Infof("ReclaimPolicy is Delete - would wipe data here")
			// TODO: Implement data wiping by mounting and running rm -rf
			// This requires creating a temporary pod to mount and wipe
		}

		// Delete Service
		err = cs.clientset.CoreV1().Services(zerofsNamespace).Delete(ctx, deploymentName, metav1.DeleteOptions{})
		if err != nil {
			klog.Warningf("Failed to delete service %s: %v", deploymentName, err)
		}

		// Delete Deployment
		err = cs.clientset.AppsV1().Deployments(zerofsNamespace).Delete(ctx, deploymentName, metav1.DeleteOptions{})
		if err != nil {
			klog.Errorf("Failed to delete deployment %s: %v", deploymentName, err)
			return nil, status.Errorf(codes.Internal, "Failed to delete deployment: %v", err)
		}

		klog.Infof("Successfully deleted ZeroFS deployment and service: %s", deploymentName)
	} else {
		klog.Infof("Keeping ZeroFS deployment %s (still has %d reference(s))", deploymentName, refCount)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume attaches a volume to a node (no-op for network filesystems)
func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	// Not needed for network filesystems - volume can be mounted directly
	return nil, status.Error(codes.Unimplemented, "ControllerPublishVolume is not supported")
}

// ControllerUnpublishVolume detaches a volume from a node (no-op for network filesystems)
func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	// Not needed for network filesystems
	return nil, status.Error(codes.Unimplemented, "ControllerUnpublishVolume is not supported")
}

// ValidateVolumeCapabilities validates volume capabilities
func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID must be provided")
	}

	caps := req.GetVolumeCapabilities()
	if len(caps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities must be provided")
	}

	klog.Infof("ValidateVolumeCapabilities: volumeID=%s", volumeID)

	// Validate that all capabilities are supported
	for _, cap := range caps {
		// We only support mount access type
		if cap.GetMount() == nil {
			return &csi.ValidateVolumeCapabilitiesResponse{
				Confirmed: nil,
				Message:   "Only mount access type is supported",
			}, nil
		}

		// Check access mode
		mode := cap.GetAccessMode().GetMode()
		if mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER &&
			mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY &&
			mode != csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY &&
			mode != csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
			return &csi.ValidateVolumeCapabilitiesResponse{
				Confirmed: nil,
				Message:   fmt.Sprintf("Unsupported access mode: %v", mode),
			}, nil
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.GetVolumeContext(),
			VolumeCapabilities: caps,
		},
	}, nil
}

// ListVolumes lists all volumes (not implemented)
func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ListVolumes is not supported")
}

// GetCapacity returns available capacity (not implemented)
func (cs *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "GetCapacity is not supported")
}

// ControllerGetCapabilities returns controller capabilities
func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(4).Infof("ControllerGetCapabilities called")

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
					},
				},
			},
		},
	}, nil
}

// CreateSnapshot creates a snapshot (not implemented)
func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "CreateSnapshot is not supported")
}

// DeleteSnapshot deletes a snapshot (not implemented)
func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "DeleteSnapshot is not supported")
}

// ListSnapshots lists snapshots (not implemented)
func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ListSnapshots is not supported")
}

// ControllerExpandVolume expands a volume (not implemented)
func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerExpandVolume is not supported")
}

// ControllerGetVolume gets volume information (not implemented)
func (cs *ControllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerGetVolume is not supported")
}

// ControllerModifyVolume modifies volume properties (not implemented)
func (cs *ControllerServer) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerModifyVolume is not supported")
}
