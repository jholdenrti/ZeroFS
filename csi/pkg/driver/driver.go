package driver

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

const (
	DefaultDriverName = "zerofs.csi.k8s.io"
)

// Driver implements the CSI plugin interfaces
type Driver struct {
	name    string
	version string
	nodeID  string

	// CSI service implementations
	ids csi.IdentityServer
	cs  csi.ControllerServer
	ns  csi.NodeServer

	// gRPC server
	srv *grpc.Server
}

// NewDriver creates a new CSI driver
func NewDriver(driverName, version, nodeID string) (*Driver, error) {
	if driverName == "" {
		driverName = DefaultDriverName
	}

	klog.Infof("Creating new ZeroFS CSI driver: %s version: %s", driverName, version)

	driver := &Driver{
		name:    driverName,
		version: version,
		nodeID:  nodeID,
	}

	// Initialize CSI service implementations
	driver.ids = NewIdentityServer(driver)
	driver.cs = NewControllerServer(driver)
	driver.ns = NewNodeServer(driver)

	return driver, nil
}

// Run starts the CSI driver gRPC server
func (d *Driver) Run(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("unable to parse endpoint: %v", err)
	}

	var addr string
	switch u.Scheme {
	case "unix":
		addr = u.Path
		// Remove existing socket file
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove unix socket %s: %v", addr, err)
		}

		// Ensure parent directory exists
		if err := os.MkdirAll(filepath.Dir(addr), 0750); err != nil {
			return fmt.Errorf("failed to create directory for socket: %v", err)
		}
	case "tcp":
		addr = u.Host
	default:
		return fmt.Errorf("unsupported protocol: %s", u.Scheme)
	}

	listener, err := net.Listen(u.Scheme, addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	// Create gRPC server
	d.srv = grpc.NewServer(
		grpc.UnaryInterceptor(logGRPC),
	)

	// Register CSI services
	csi.RegisterIdentityServer(d.srv, d.ids)
	csi.RegisterControllerServer(d.srv, d.cs)
	csi.RegisterNodeServer(d.srv, d.ns)

	klog.Infof("Listening for connections on address: %s", addr)
	return d.srv.Serve(listener)
}

// Stop gracefully stops the driver
func (d *Driver) Stop() {
	if d.srv != nil {
		klog.Info("Stopping gRPC server")
		d.srv.GracefulStop()
	}
}

// logGRPC is a gRPC unary interceptor for logging
func logGRPC(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	klog.V(5).Infof("GRPC call: %s", info.FullMethod)
	klog.V(6).Infof("GRPC request: %+v", req)

	resp, err := handler(ctx, req)

	if err != nil {
		klog.Errorf("GRPC error: %v", err)
	} else {
		klog.V(6).Infof("GRPC response: %+v", resp)
	}

	return resp, err
}

// GetName returns the driver name
func (d *Driver) GetName() string {
	return d.name
}

// GetVersion returns the driver version
func (d *Driver) GetVersion() string {
	return d.version
}

// GetNodeID returns the node ID
func (d *Driver) GetNodeID() string {
	return d.nodeID
}
