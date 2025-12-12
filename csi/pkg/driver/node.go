package driver

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/rti/zerofs/csi/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// NodeServer implements the CSI Node service
type NodeServer struct {
	driver *Driver
	csi.UnimplementedNodeServer
}

// NewNodeServer creates a new Node server
func NewNodeServer(driver *Driver) csi.NodeServer {
	return &NodeServer{
		driver: driver,
	}
}

// NodeStageVolume stages a volume to a staging path
func (ns *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID must be provided")
	}

	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Staging target path must be provided")
	}

	volumeContext := req.GetVolumeContext()
	server := volumeContext["server"]
	if server == "" {
		return nil, status.Error(codes.InvalidArgument, "server not found in volume context")
	}

	port := volumeContext["port"]
	if port == "" {
		port = defaultPort
	}

	path := volumeContext["path"]

	klog.Infof("NodeStageVolume: volumeID=%s server=%s port=%s path=%s target=%s",
		volumeID, server, port, path, stagingTargetPath)

	// Mount the 9P filesystem to staging path
	mountOpts := &util.MountOptions{
		Server: server,
		Port:   port,
		Path:   path,
		Target: stagingTargetPath,
	}

	// Add any custom mount flags from volume capability
	if req.GetVolumeCapability() != nil && req.GetVolumeCapability().GetMount() != nil {
		mountOpts.MountFlags = req.GetVolumeCapability().GetMount().GetMountFlags()
	}

	if err := util.Mount9P(mountOpts); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to mount volume: %v", err)
	}

	klog.Infof("Successfully staged volume %s at %s", volumeID, stagingTargetPath)
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume unstages a volume from a staging path
func (ns *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID must be provided")
	}

	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Staging target path must be provided")
	}

	klog.Infof("NodeUnstageVolume: volumeID=%s target=%s", volumeID, stagingTargetPath)

	// Unmount the volume
	if err := util.Unmount9P(stagingTargetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to unmount volume: %v", err)
	}

	// Cleanup the staging directory
	if err := util.CleanupMountPoint(stagingTargetPath); err != nil {
		klog.Warningf("Failed to cleanup staging path %s: %v", stagingTargetPath, err)
	}

	klog.Infof("Successfully unstaged volume %s from %s", volumeID, stagingTargetPath)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume mounts a volume to the target path
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID must be provided")
	}

	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Staging target path must be provided")
	}

	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Target path must be provided")
	}

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability must be provided")
	}

	// Get path parameter for subdirectory mounting
	volumeContext := req.GetVolumeContext()
	subPath := volumeContext["path"]
	if subPath == "" {
		subPath = "/"
	}

	klog.Infof("NodePublishVolume: volumeID=%s staging=%s target=%s path=%s",
		volumeID, stagingTargetPath, targetPath, subPath)

	// Ensure target path exists
	if err := util.EnsureDirectory(targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to create target directory: %v", err)
	}

	// Check if already mounted
	isMounted, err := util.IsMountPoint(targetPath)
	if err != nil && err.Error() != "path does not exist" {
		return nil, status.Errorf(codes.Internal, "Failed to check if target is mounted: %v", err)
	}

	if isMounted {
		klog.Infof("Target %s is already mounted", targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// Determine source path (staging + subPath)
	var source string
	if subPath == "/" {
		// Mount entire volume
		source = stagingTargetPath
	} else {
		// Mount subdirectory only
		source = filepath.Join(stagingTargetPath, subPath)

		// Auto-create subdirectory if it doesn't exist (like Docker volumes)
		if _, err := os.Stat(source); err != nil {
			if os.IsNotExist(err) {
				klog.Infof("Creating subdirectory path: %s", subPath)
				if err := os.MkdirAll(source, 0755); err != nil {
					return nil, status.Errorf(codes.Internal,
						"Failed to create path '%s': %v", subPath, err)
				}
				klog.Infof("Successfully created subdirectory: %s", subPath)
			} else {
				return nil, status.Errorf(codes.Internal,
					"Failed to verify path '%s': %v", subPath, err)
			}
		}

		klog.Infof("Mounting subdirectory: %s", subPath)
	}

	// Bind mount from source (staging + subPath) to target
	// We use bind mount to propagate the already-mounted staging path to the target
	mountOpts := []string{"bind"}
	if req.GetReadonly() {
		mountOpts = append(mountOpts, "ro")
	}

	klog.Infof("Bind mounting %s to %s with options: %v", source, targetPath, mountOpts)

	// For bind mount, we need to use mount command directly
	if err := bindMount(source, targetPath, mountOpts); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to bind mount: %v", err)
	}

	klog.Infof("Successfully published volume %s to %s (path: %s)", volumeID, targetPath, subPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts a volume from the target path
func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID must be provided")
	}

	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Target path must be provided")
	}

	klog.Infof("NodeUnpublishVolume: volumeID=%s target=%s", volumeID, targetPath)

	// Unmount the target path
	if err := util.Unmount9P(targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to unmount: %v", err)
	}

	// Cleanup the target directory
	if err := util.CleanupMountPoint(targetPath); err != nil {
		klog.Warningf("Failed to cleanup target path %s: %v", targetPath, err)
	}

	klog.Infof("Successfully unpublished volume %s from %s", volumeID, targetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetVolumeStats returns volume statistics
func (ns *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "NodeGetVolumeStats is not implemented")
}

// NodeExpandVolume expands a volume
func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "NodeExpandVolume is not implemented")
}

// NodeGetCapabilities returns node capabilities
func (ns *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	klog.V(4).Infof("NodeGetCapabilities called")

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
		},
	}, nil
}

// NodeGetInfo returns node information
func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	klog.V(4).Infof("NodeGetInfo called")

	return &csi.NodeGetInfoResponse{
		NodeId: ns.driver.GetNodeID(),
		// No topology constraints for network filesystem
		AccessibleTopology: nil,
	}, nil
}

// bindMount performs a bind mount
func bindMount(source, target string, options []string) error {
	klog.Infof("Executing bind mount: %s -> %s with options: %v", source, target, options)

	// Build mount command for bind mount
	args := []string{"--bind"}

	for _, opt := range options {
		if opt == "ro" {
			args = append(args, "-o", "ro")
		}
	}

	args = append(args, source, target)

	if err := executeMount(args); err != nil {
		return fmt.Errorf("bind mount failed: %v", err)
	}

	return nil
}

// executeMount executes the mount command
func executeMount(args []string) error {
	cmd := fmt.Sprintf("mount %s", joinArgs(args))
	klog.V(4).Infof("Executing: %s", cmd)

	execCmd := exec.Command("mount", args...)
	output, err := execCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v, output: %s", err, string(output))
	}

	return nil
}

// joinArgs joins command arguments for logging
func joinArgs(args []string) string {
	result := ""
	for _, arg := range args {
		if result != "" {
			result += " "
		}
		result += arg
	}
	return result
}
