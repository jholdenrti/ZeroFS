package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	zerofsNamespace    = "zerofs-system"
	zerofsImage        = "zerofs:latest"
	zerofsPort         = 5564
	annotationDeployment = "zerofs.csi.k8s.io/deployment"
	annotationURL       = "zerofs.csi.k8s.io/url"
)

// StorageConfig contains all parameters needed to identify unique storage backend
type StorageConfig struct {
	URL                string // Required: s3://bucket/path or az://container/path
	StorageAccountName string // Optional: Azure storage account name
	Region             string // Optional: S3 region
	Endpoint           string // Optional: S3 endpoint
	CredentialsSecret  string // Optional: Secret name containing credentials
	EncryptionSecret   string // Optional: Secret name containing encryption password
	CacheSize          string // Optional: Cache size (e.g., "1Gi")
}

// getStorageConfigFromParams extracts storage configuration from volume parameters
func getStorageConfigFromParams(params map[string]string) (*StorageConfig, error) {
	url := params["url"]
	if url == "" {
		return nil, fmt.Errorf("url parameter is required")
	}

	return &StorageConfig{
		URL:                url,
		StorageAccountName: params["storageAccountName"],
		Region:             params["region"],
		Endpoint:           params["endpoint"],
		CredentialsSecret:  params["credentialsSecret"],
		EncryptionSecret:   params["encryptionSecret"],
		CacheSize:          params["cacheSize"],
	}, nil
}

// getDeploymentName generates a deterministic deployment name from storage config
// Hash is based on: url + storageAccountName + region + endpoint
func getDeploymentName(config *StorageConfig) string {
	// Build hash input from all uniqueness parameters
	hashParts := []string{config.URL}

	if config.StorageAccountName != "" {
		hashParts = append(hashParts, "account:"+config.StorageAccountName)
	}
	if config.Region != "" {
		hashParts = append(hashParts, "region:"+config.Region)
	}
	if config.Endpoint != "" {
		hashParts = append(hashParts, "endpoint:"+config.Endpoint)
	}

	// Sort for deterministic hash
	sort.Strings(hashParts)

	// Create hash
	hashInput := strings.Join(hashParts, "|")
	hash := sha256.Sum256([]byte(hashInput))
	shortHash := hex.EncodeToString(hash[:8]) // 16 characters

	return fmt.Sprintf("zerofs-%s", shortHash)
}

// getOrCreateZeroFSDeployment ensures a ZeroFS deployment exists for the given storage config
func (cs *ControllerServer) getOrCreateZeroFSDeployment(ctx context.Context, clientset *kubernetes.Clientset, config *StorageConfig) (string, error) {
	deploymentName := getDeploymentName(config)

	klog.Infof("Ensuring ZeroFS deployment: %s for URL: %s", deploymentName, config.URL)

	// Check if deployment already exists
	_, err := clientset.AppsV1().Deployments(zerofsNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err == nil {
		klog.Infof("ZeroFS deployment %s already exists, reusing", deploymentName)
		return deploymentName, nil
	}

	if !errors.IsNotFound(err) {
		return "", fmt.Errorf("failed to check deployment: %v", err)
	}

	// Deployment doesn't exist - create it
	klog.Infof("Creating new ZeroFS deployment: %s", deploymentName)

	deployment := cs.buildZeroFSDeployment(deploymentName, config)
	_, err = clientset.AppsV1().Deployments(zerofsNamespace).Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			// Race condition - another controller created it
			klog.Infof("Deployment was created concurrently, using existing: %s", deploymentName)
			return deploymentName, nil
		}
		return "", fmt.Errorf("failed to create deployment: %v", err)
	}

	// Create corresponding service
	service := cs.buildZeroFSService(deploymentName)
	_, err = clientset.CoreV1().Services(zerofsNamespace).Create(ctx, service, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		klog.Errorf("Failed to create service for %s: %v", deploymentName, err)
		// Don't fail - service might have been created by another controller
	}

	klog.Infof("Successfully created ZeroFS deployment and service: %s", deploymentName)
	return deploymentName, nil
}

// buildZeroFSDeployment creates the Deployment spec for a ZeroFS instance
func (cs *ControllerServer) buildZeroFSDeployment(name string, config *StorageConfig) *appsv1.Deployment {
	replicas := int32(1)

	labels := map[string]string{
		"app":       "zerofs",
		"component": "storage",
		"managed-by": "zerofs-csi-driver",
	}

	// Build environment variables
	env := []corev1.EnvVar{
		{
			Name:  "STORAGE_URL",
			Value: config.URL,
		},
	}

	// Add Azure-specific env vars
	if config.StorageAccountName != "" {
		env = append(env, corev1.EnvVar{
			Name:  "AZURE_STORAGE_ACCOUNT_NAME",
			Value: config.StorageAccountName,
		})
	}

	// Add S3-specific env vars
	if config.Region != "" {
		env = append(env, corev1.EnvVar{
			Name:  "AWS_REGION",
			Value: config.Region,
		})
	}
	if config.Endpoint != "" {
		env = append(env, corev1.EnvVar{
			Name:  "AWS_ENDPOINT",
			Value: config.Endpoint,
		})
	}

	// Add credentials from secret if specified
	if config.CredentialsSecret != "" {
		// AWS credentials
		env = append(env,
			corev1.EnvVar{
				Name: "AWS_ACCESS_KEY_ID",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: config.CredentialsSecret,
						},
						Key: "access-key-id",
					},
				},
			},
			corev1.EnvVar{
				Name: "AWS_SECRET_ACCESS_KEY",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: config.CredentialsSecret,
						},
						Key: "secret-access-key",
					},
				},
			},
		)

		// Azure credentials (same secret, different keys)
		env = append(env, corev1.EnvVar{
			Name: "AZURE_STORAGE_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: config.CredentialsSecret,
					},
					Key:      "azure-storage-key",
					Optional: boolPtr(true),
				},
			},
		})
	}

	// Add encryption password from secret
	if config.EncryptionSecret != "" {
		env = append(env, corev1.EnvVar{
			Name: "ENCRYPTION_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: config.EncryptionSecret,
					},
					Key: "password",
				},
			},
		})
	} else {
		// Use a default/generated password if none specified
		env = append(env, corev1.EnvVar{
			Name:  "ENCRYPTION_PASSWORD",
			Value: "default-change-me",
		})
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: zerofsNamespace,
			Labels:    labels,
			Annotations: map[string]string{
				annotationURL: config.URL,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":  "zerofs",
					"name": name,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":  "zerofs",
						"name": name,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "zerofs",
							Image: zerofsImage,
							Command: []string{"zerofs"},
							Args: []string{
								"run",
								"-c", "/etc/zerofs/config.toml",
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "ninep",
									ContainerPort: zerofsPort,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							Env: env,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    parseQuantity("100m"),
									corev1.ResourceMemory: parseQuantity("256Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    parseQuantity("1000m"),
									corev1.ResourceMemory: parseQuantity("2Gi"),
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromInt(zerofsPort),
									},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       30,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromInt(zerofsPort),
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
							},
						},
					},
				},
			},
		},
	}

	return deployment
}

// buildZeroFSService creates the Service spec for a ZeroFS instance
func (cs *ControllerServer) buildZeroFSService(name string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: zerofsNamespace,
			Labels: map[string]string{
				"app":       "zerofs",
				"component": "storage",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app":  "zerofs",
				"name": name,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "ninep",
					Protocol:   corev1.ProtocolTCP,
					Port:       zerofsPort,
					TargetPort: intstr.FromInt(zerofsPort),
				},
			},
			Type: corev1.ServiceTypeClusterIP,
		},
	}
}

// countPVsReferencingDeployment counts how many PVs reference a given deployment
func (cs *ControllerServer) countPVsReferencingDeployment(ctx context.Context, clientset *kubernetes.Clientset, deploymentName string) (int, error) {
	pvList, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to list PVs: %v", err)
	}

	count := 0
	for _, pv := range pvList.Items {
		// Check if this PV uses our CSI driver
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == DefaultDriverName {
			// Check annotation for deployment name
			if pv.Annotations[annotationDeployment] == deploymentName {
				count++
			}
		}
	}

	klog.V(4).Infof("Found %d PVs referencing deployment %s", count, deploymentName)
	return count, nil
}

// Helper functions
func int32Ptr(i int32) *int32 {
	return &i
}

func boolPtr(b bool) *bool {
	return &b
}

func parseQuantity(s string) resource.Quantity {
	q, _ := resource.ParseQuantity(s)
	return q
}
