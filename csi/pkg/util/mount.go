package util

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
)

// MountOptions contains options for mounting 9P filesystems
type MountOptions struct {
	Server      string
	Port        string
	Path        string
	Target      string
	MountFlags  []string
	FSType      string
}

// Mount9P mounts a 9P filesystem
func Mount9P(opts *MountOptions) error {
	mounter := mount.New("")

	// Check if already mounted
	isMounted, err := mounter.IsMountPoint(opts.Target)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to check mount point: %v", err)
		}
		// Target doesn't exist, that's ok
		isMounted = false
	}

	if isMounted {
		klog.Infof("Target %s is already mounted", opts.Target)
		return nil
	}

	// Ensure target directory exists
	if err := os.MkdirAll(opts.Target, 0755); err != nil {
		return fmt.Errorf("failed to create target directory: %v", err)
	}

	// Default port to 5564 if not specified
	port := opts.Port
	if port == "" {
		port = "5564"
	}

	// Build mount options
	mountOpts := []string{
		"trans=tcp",
		fmt.Sprintf("port=%s", port),
		"version=9p2000.L",
		"cache=mmap",
		"access=user",
	}

	// Add custom mount flags if provided
	if len(opts.MountFlags) > 0 {
		mountOpts = append(mountOpts, opts.MountFlags...)
	}

	// Add path if specified (for subdirectory mounting)
	source := opts.Server
	if opts.Path != "" {
		// For 9P, we'll mount the root and bind-mount the subdirectory
		// Or use aname parameter for the path
		mountOpts = append(mountOpts, fmt.Sprintf("aname=%s", opts.Path))
	}

	klog.Infof("Mounting 9P: source=%s target=%s options=%v", source, opts.Target, mountOpts)

	// Execute mount command
	args := []string{
		"-t", "9p",
		"-o", strings.Join(mountOpts, ","),
		source,
		opts.Target,
	}

	cmd := exec.Command("mount", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount command failed: %v, output: %s", err, string(output))
	}

	klog.Infof("Successfully mounted %s at %s", source, opts.Target)
	return nil
}

// Unmount9P unmounts a 9P filesystem
func Unmount9P(target string) error {
	mounter := mount.New("")

	klog.Infof("Unmounting %s", target)

	// Check if mounted
	isMounted, err := mounter.IsMountPoint(target)
	if err != nil {
		if os.IsNotExist(err) {
			klog.Infof("Mount point %s does not exist, nothing to unmount", target)
			return nil
		}
		klog.Warningf("Failed to check if %s is a mount point: %v", target, err)
	}

	if !isMounted {
		klog.Infof("Target %s is not mounted, skipping unmount", target)
		return nil
	}

	// Unmount
	if err := mounter.Unmount(target); err != nil {
		return fmt.Errorf("unmount failed: %v", err)
	}

	klog.Infof("Successfully unmounted %s", target)
	return nil
}

// IsMountPoint checks if a path is a mount point
func IsMountPoint(path string) (bool, error) {
	mounter := mount.New("")
	return mounter.IsMountPoint(path)
}

// EnsureDirectory creates a directory if it doesn't exist
func EnsureDirectory(path string) error {
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", path, err)
	}
	return nil
}

// CleanupMountPoint removes a directory if it's empty and not mounted
func CleanupMountPoint(path string) error {
	isMounted, err := IsMountPoint(path)
	if err != nil {
		return err
	}

	if isMounted {
		return fmt.Errorf("cannot cleanup: %s is still mounted", path)
	}

	// Try to remove the directory
	if err := os.Remove(path); err != nil {
		if !os.IsNotExist(err) {
			klog.Warningf("Failed to remove mount point %s: %v", path, err)
		}
	}

	return nil
}
