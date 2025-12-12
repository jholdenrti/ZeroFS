package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/rti/zerofs/csi/pkg/driver"
	"k8s.io/klog/v2"
)

var (
	endpoint        = flag.String("endpoint", "unix:///csi/csi.sock", "CSI endpoint")
	nodeID          = flag.String("nodeid", "", "Node ID")
	driverName      = flag.String("drivername", "zerofs.csi.k8s.io", "Name of the driver")
	version         = flag.String("version", "v1.0.0", "Version of the driver")
	showVersion     = flag.Bool("show-version", false, "Show version and exit")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	if *showVersion {
		fmt.Printf("ZeroFS CSI Driver\nVersion: %s\n", *version)
		os.Exit(0)
	}

	if *nodeID == "" {
		klog.Fatal("Node ID must be provided")
	}

	klog.Infof("Starting ZeroFS CSI Driver")
	klog.Infof("Driver: %s Version: %s", *driverName, *version)
	klog.Infof("Node ID: %s", *nodeID)

	drv, err := driver.NewDriver(*driverName, *version, *nodeID)
	if err != nil {
		klog.Fatalf("Failed to create driver: %v", err)
	}

	if err := drv.Run(*endpoint); err != nil {
		klog.Fatalf("Failed to run driver: %v", err)
	}
}
