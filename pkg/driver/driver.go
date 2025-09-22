/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/util"
)

const (
	driverName = "efs.csi.aws.com"

	// AgentNotReadyTaintKey contains the key of taints to be removed on driver startup
	AgentNotReadyNodeTaintKey = "efs.csi.aws.com/agent-not-ready"
)

type Driver struct {
	endpoint                 string
	nodeID                   string
	srv                      *grpc.Server
	mounter                  Mounter
	efsWatchdog              Watchdog
	cloud                    cloud.Cloud
	nodeCaps                 []csi.NodeServiceCapability_RPC_Type
	volMetricsOptIn          bool
	volMetricsRefreshPeriod  float64
	volMetricsFsRateLimit    int
	volStatter               VolStatter
	gidAllocator             GidAllocator
	deleteAccessPointRootDir bool
	adaptiveRetryMode        bool
	tags                     map[string]string
	lockManager              LockManagerMap
	namespaceProvisioner     NamespaceProvisionerInterface
}

func NewDriver(endpoint, efsUtilsCfgPath, efsUtilsStaticFilesPath, tags string, volMetricsOptIn bool, volMetricsRefreshPeriod float64, volMetricsFsRateLimit int, deleteAccessPointRootDir bool, adaptiveRetryMode bool) *Driver {
	cloud, err := cloud.NewCloud(adaptiveRetryMode)
	if err != nil {
		klog.Fatalln(err)
	}

	nodeCaps := SetNodeCapOptInFeatures(volMetricsOptIn)
	watchdog := newExecWatchdog(efsUtilsCfgPath, efsUtilsStaticFilesPath, "amazon-efs-mount-watchdog")
	return &Driver{
		endpoint:                 endpoint,
		nodeID:                   cloud.GetMetadata().GetInstanceID(),
		mounter:                  newNodeMounter(),
		efsWatchdog:              watchdog,
		cloud:                    cloud,
		nodeCaps:                 nodeCaps,
		volStatter:               NewVolStatter(),
		volMetricsOptIn:          volMetricsOptIn,
		volMetricsRefreshPeriod:  volMetricsRefreshPeriod,
		volMetricsFsRateLimit:    volMetricsFsRateLimit,
		gidAllocator:             NewGidAllocator(),
		deleteAccessPointRootDir: deleteAccessPointRootDir,
		adaptiveRetryMode:        adaptiveRetryMode,
		tags:                     parseTagsFromStr(strings.TrimSpace(tags)),
		lockManager:              NewLockManagerMap(),
		namespaceProvisioner:     nil, // Will be initialized when needed
	}
}

// SetNamespaceProvisioner sets the namespace provisioner for the driver
func (d *Driver) SetNamespaceProvisioner(namespaceProvisioner NamespaceProvisionerInterface) {
	d.namespaceProvisioner = namespaceProvisioner
}

// InitializeNamespaceProvisioner initializes the namespace provisioner if not already initialized
// This is called lazily when the first efs-ns mode volume is requested
func (d *Driver) InitializeNamespaceProvisioner() error {
	if d.namespaceProvisioner != nil {
		klog.V(4).Infof("NamespaceProvisioner already initialized")
		return nil
	}

	klog.Info("Initializing NamespaceProvisioner for efs-ns mode")

	// Get Kubernetes client
	k8sClient, err := cloud.DefaultKubernetesAPIClient()
	if err != nil {
		return fmt.Errorf("failed to get Kubernetes client: %w", err)
	}

	// Get Kubernetes config
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get Kubernetes config: %w", err)
	}

	// Create provisioner options with default values
	// These can be customized later based on StorageClass parameters
	options := DefaultProvisionerOptions()

	// Set cluster-wide tags from the Driver
	options.DefaultTags = d.tags

	// Set cluster ID if available
	if clusterID := os.Getenv("CLUSTER_NAME"); clusterID != "" {
		options.ClusterID = clusterID
	}

	// Set region from metadata
	metadata := d.cloud.GetMetadata()
	options.Region = metadata.GetRegion()

	// Initialize the NamespaceProvisioner
	provisioner, err := NewNamespaceProvisioner(d.cloud, k8sClient, config, options)
	if err != nil {
		return fmt.Errorf("failed to create NamespaceProvisioner: %w", err)
	}

	// Start the provisioner
	ctx := context.Background()
	if err := provisioner.Start(ctx); err != nil {
		return fmt.Errorf("failed to start NamespaceProvisioner: %w", err)
	}

	// Set the provisioner in the driver
	d.namespaceProvisioner = provisioner

	klog.Info("NamespaceProvisioner initialized successfully")
	return nil
}

// GetNamespaceProvisioner returns the namespace provisioner, initializing it if necessary
func (d *Driver) GetNamespaceProvisioner() (NamespaceProvisionerInterface, error) {
	if d.namespaceProvisioner == nil {
		if err := d.InitializeNamespaceProvisioner(); err != nil {
			return nil, err
		}
	}
	return d.namespaceProvisioner, nil
}

func SetNodeCapOptInFeatures(volMetricsOptIn bool) []csi.NodeServiceCapability_RPC_Type {
	var nCaps = []csi.NodeServiceCapability_RPC_Type{}
	if volMetricsOptIn {
		klog.V(4).Infof("Enabling Node Service capability for Get Volume Stats")
		nCaps = append(nCaps, csi.NodeServiceCapability_RPC_GET_VOLUME_STATS)
	} else {
		klog.V(4).Infof("Node Service capability for Get Volume Stats Not enabled")
	}
	return nCaps
}

func (d *Driver) Run() error {
	scheme, addr, err := util.ParseEndpoint(d.endpoint)
	if err != nil {
		return err
	}

	listener, err := net.Listen(scheme, addr)
	if err != nil {
		return err
	}

	logErr := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			klog.Errorf("GRPC error: %v", err)
		}
		return resp, err
	}
	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(logErr),
	}
	d.srv = grpc.NewServer(opts...)

	csi.RegisterIdentityServer(d.srv, d)
	klog.Info("Registering Node Server")
	csi.RegisterNodeServer(d.srv, d)
	klog.Info("Registering Controller Server")
	csi.RegisterControllerServer(d.srv, d)

	klog.Info("Starting efs-utils watchdog")
	if err := d.efsWatchdog.start(); err != nil {
		return err
	}

	reaper := newReaper()
	klog.Info("Starting reaper")
	reaper.start()

	// Remove taint from node to indicate driver startup success
	// This is done at the last possible moment to prevent race conditions or false positive removals
	go tryRemoveNotReadyTaintUntilSucceed(time.Second, func() error {
		return removeNotReadyTaint(cloud.DefaultKubernetesAPIClient)
	})

	klog.Infof("Listening for connections on address: %#v", listener.Addr())
	return d.srv.Serve(listener)
}

// Stop gracefully stops the CSI driver and cleans up resources
func (d *Driver) Stop() {
	klog.Info("Stopping CSI driver")

	// Stop the NamespaceProvisioner if it was initialized
	if d.namespaceProvisioner != nil {
		klog.Info("Stopping NamespaceProvisioner")
		if err := d.namespaceProvisioner.Stop(); err != nil {
			klog.Errorf("Error stopping NamespaceProvisioner: %v", err)
		}
	}

	// Stop the gRPC server if it's running
	if d.srv != nil {
		d.srv.GracefulStop()
	}

	klog.Info("CSI driver stopped")
}

func parseTagsFromStr(tagStr string) map[string]string {
	defer func() {
		if r := recover(); r != nil {
			klog.Errorf("Failed to parse input tag string: %v", tagStr)
		}
	}()

	m := make(map[string]string)
	if tagStr == "" {
		klog.Infof("Did not find any input tags.")
		return m
	}
	tagsSplit := strings.Split(tagStr, " ")
	for _, pair := range tagsSplit {
		p := strings.Split(pair, ":")
		m[p[0]] = p[1]
	}
	return m
}
