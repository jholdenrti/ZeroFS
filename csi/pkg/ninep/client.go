package ninep

import (
	"fmt"
	"os/exec"
	"strings"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
)

// Client represents a 9P client for mounting ZeroFS
type Client struct {
	server string
	port   string
	mounter mount.Interface
}

// NewClient creates a new 9P client
func NewClient(server, port string) *Client {
	return &Client{
		server:  server,
		port:    port,
		mounter: mount.New(""),
	}
}

// Mount mounts the 9P filesystem at the specified mount point
func (c *Client) Mount(mountPoint string) (bool, error) {
	// Check if already mounted
	isMounted, err := c.mounter.IsMountPoint(mountPoint)
	if err != nil {
		klog.V(4).Infof("Checking mount point %s: %v", mountPoint, err)
	}

	if isMounted {
		klog.Infof("Already mounted at %s", mountPoint)
		return true, nil
	}

	// Build mount options
	options := []string{
		"trans=tcp",
		fmt.Sprintf("port=%s", c.port),
		"version=9p2000.L",
		"cache=mmap",
		"access=user",
	}

	klog.Infof("Mounting 9P filesystem: server=%s port=%s target=%s", c.server, c.port, mountPoint)

	// Execute mount command
	// mount -t 9p -o trans=tcp,port=5564,version=9p2000.L,cache=mmap,access=user <server> <mountPoint>
	args := []string{
		"-t", "9p",
		"-o", strings.Join(options, ","),
		c.server,
		mountPoint,
	}

	cmd := exec.Command("mount", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("mount failed: %v, output: %s", err, string(output))
	}

	klog.Infof("Successfully mounted %s at %s", c.server, mountPoint)
	return true, nil
}

// Unmount unmounts the 9P filesystem
func (c *Client) Unmount(mountPoint string) error {
	klog.Infof("Unmounting %s", mountPoint)

	// Check if mounted
	isMounted, err := c.mounter.IsMountPoint(mountPoint)
	if err != nil {
		klog.V(4).Infof("Checking mount point %s: %v", mountPoint, err)
	}

	if !isMounted {
		klog.Infof("Not mounted at %s, skipping unmount", mountPoint)
		return nil
	}

	// Unmount
	if err := c.mounter.Unmount(mountPoint); err != nil {
		return fmt.Errorf("unmount failed: %v", err)
	}

	klog.Infof("Successfully unmounted %s", mountPoint)
	return nil
}
