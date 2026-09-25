/*
Copyright 2025 SUSE.

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

package util

import (
	"context"
	"encoding/base64"
	"net"
	re "regexp"
	"strings"
	"time"

	"github.com/pkg/errors"
	"sigs.k8s.io/json"
	"sigs.k8s.io/yaml"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	machineryyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	clientcmdlatest "k8s.io/client-go/tools/clientcmd/api/latest"

	lbclient "github.com/harvester/cluster-api-provider-harvester/pkg/clientset/versioned"
)

const (
	readerBufferSize      = 4096
	cloudProviderRoleName = "harvesterhci.io:cloudprovider"
	maxNumberOfSecrets    = 15

	harvesterVIPServiceNamespace     = "kube-system"
	legacyVIPServiceName             = "ingress-expose"
	traefikVIPServiceName            = "rke2-traefik"
	kubeVIPLoadBalancerIPsAnnotation = "kube-vip.io/loadbalancerIPs"
)

// GetCloudConfigB64 returns the kubeconfig for the service account.
func GetCloudConfigB64(ctx context.Context, hvClient lbclient.Interface, saName string, namespace string, harvesterServerURL string) (string, error) {
	err := createServiceAccountIfNotExists(ctx, hvClient, saName, namespace)
	if err != nil {
		return "", err
	}

	err = createClusterRoleBindingIfNotExists(ctx, hvClient, saName, namespace)
	if err != nil {
		return "", err
	}

	kubeconfig, err := getKubeConfig(ctx, hvClient, saName, namespace, harvesterServerURL)

	return kubeconfig, err
}

// createServiceAccountIfNotExists creates a service account if it does not exist.
func createServiceAccountIfNotExists(ctx context.Context, hvClient lbclient.Interface, saName string, namespace string) error {
	_, err := hvClient.CoreV1().ServiceAccounts(namespace).Get(ctx, saName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}

		serviceAccount := &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name: saName,
			},
		}

		_, err := hvClient.CoreV1().ServiceAccounts(namespace).Create(ctx, serviceAccount, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	}

	return nil
}

// createClusterRoleBindingIfNotExists creates a cluster role binding for the Cloud Provider's ServiceAccount if it does not exist.
func createClusterRoleBindingIfNotExists(ctx context.Context, hvClient lbclient.Interface, saName string, namespace string) error {
	_, err := hvClient.RbacV1().ClusterRoleBindings().Get(ctx, saName, metav1.GetOptions{})
	if err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	clusterRoleBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: saName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      saName,
				Namespace: namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Kind: "ClusterRole",
			Name: cloudProviderRoleName,
		},
	}

	_, err = hvClient.RbacV1().ClusterRoleBindings().Create(ctx, clusterRoleBinding, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	return err
}

// getKubeConfig returns a kubeconfig from the Secret associated with the ServiceAccount.
func getKubeConfig(ctx context.Context, hvClient lbclient.Interface, saName string, namespace string, harvesterServerURL string) (string, error) {
	sa, err := hvClient.CoreV1().ServiceAccounts(namespace).Get(ctx, saName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	serviceAccountUID := string(sa.UID)
	serviceAccountName := sa.Name
	secretName := serviceAccountName + "-token"

	// Create a secret for the service account
	_, err = hvClient.CoreV1().Secrets(namespace).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: secretName,
			Annotations: map[string]string{
				corev1.ServiceAccountNameKey: serviceAccountName,
				corev1.ServiceAccountUIDKey:  serviceAccountUID,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "ServiceAccount",
					Name:       serviceAccountName,
					UID:        sa.UID,
				},
			},
		},
		Type: corev1.SecretTypeServiceAccountToken,
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return "", err
	}

	time.Sleep(time.Second)

	secret, err := hvClient.CoreV1().Secrets(namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	vipIP, err := getHarvesterVIP(ctx, hvClient)
	if err != nil {
		return "", err
	}

	if vipIP != "" {
		harvesterServerURL = "https://" + net.JoinHostPort(vipIP, "6443")
	}

	kubeconfig, err := buildKubeconfigFromSecret(secret, namespace, harvesterServerURL)
	if err != nil {
		return "", errors.Errorf("unable to build a kubeconfig from secret %s", saName)
	}

	return base64.StdEncoding.EncodeToString([]byte(kubeconfig)), nil
}

// getHarvesterVIP returns the IPv4 address of the Harvester management VIP, read from
// the Services that kube-vip exposes in kube-system.
//
// Harvester up to v1.8 exposes the VIP as the LoadBalancer address of the ingress-expose
// Service. Harvester v1.9 removed that Service: the VIP is the LoadBalancer address of the
// rke2-traefik Service. ingress-expose is read first and, when it exists, keeps its current
// behavior: an error when no address is allocated yet, "" (the caller keeps the server URL
// it was given) when the address is not shaped like IPv4. rke2-traefik is only consulted
// when ingress-expose does not exist, and an error is returned when it yields no IPv4
// address either (the LoadBalancer may still be pending: the caller requeues).
func getHarvesterVIP(ctx context.Context, hvClient lbclient.Interface) (string, error) {
	services := hvClient.CoreV1().Services(harvesterVIPServiceNamespace)

	legacySVC, err := services.Get(ctx, legacyVIPServiceName, metav1.GetOptions{})
	if err == nil {
		return legacyVIPFromService(legacySVC)
	}

	if !apierrors.IsNotFound(err) {
		return "", errors.Wrap(err, "unable to compute the Harvester Endpoint: problem in getting the ingress-expose service")
	}

	traefikSVC, err := services.Get(ctx, traefikVIPServiceName, metav1.GetOptions{})
	if err != nil {
		return "", errors.Wrap(err, "unable to compute the Harvester Endpoint: the ingress-expose service (Harvester < v1.9) "+
			"does not exist and the rke2-traefik service (Harvester >= v1.9) could not be read")
	}

	for _, ingress := range traefikSVC.Status.LoadBalancer.Ingress {
		if ip := firstIPv4(ingress.IP); ip != "" {
			return ip, nil
		}
	}

	if ip := firstIPv4(traefikSVC.Annotations[kubeVIPLoadBalancerIPsAnnotation]); ip != "" {
		return ip, nil
	}

	return "", errors.New("unable to compute the Harvester Endpoint: the ingress-expose service (Harvester < v1.9) does not exist " +
		"and the rke2-traefik service (Harvester >= v1.9) has no IPv4 LoadBalancer address yet")
}

// legacyVIPFromService reads the VIP from the ingress-expose Service the way Harvester
// < v1.9 publishes it: the first LoadBalancer address, used as is when it is shaped like
// IPv4.
func legacyVIPFromService(svc *corev1.Service) (string, error) {
	if len(svc.Status.LoadBalancer.Ingress) == 0 {
		return "", errors.New("unable to compute the Harvester Endpoint: no ip allocated in the ingress-expose service")
	}

	vipIP := svc.Status.LoadBalancer.Ingress[0].IP

	ok, err := re.MatchString(`\d+\.\d+\.\d+\.\d+`, vipIP)
	if ok && err == nil {
		return vipIP, nil
	}

	return "", nil
}

// firstIPv4 returns the first usable IPv4 address of a comma-separated list (the format of
// the kube-vip.io/loadbalancerIPs annotation, also valid for a single address), or "".
func firstIPv4(value string) string {
	for candidate := range strings.SplitSeq(value, ",") {
		ip := net.ParseIP(strings.TrimSpace(candidate))
		if ip != nil && ip.To4() != nil && !ip.IsUnspecified() {
			return ip.String()
		}
	}

	return ""
}

// buildKubeconfigFromSecret builds a kubeconfig from a secret content.
func buildKubeconfigFromSecret(secret *corev1.Secret, namespace string, harvesterServerURL string) (string, error) {
	token, ok := secret.Data[corev1.ServiceAccountTokenKey]
	if !ok {
		return "", errors.New("token not found in secret")
	}

	ca, ok := secret.Data[corev1.ServiceAccountRootCAKey]
	if !ok {
		return "", errors.New("ca.crt not found in secret")
	}

	kubeconfigObject := &clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			"default": {
				Server:                   harvesterServerURL,
				CertificateAuthorityData: ca,
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"default": {
				Token: string(token),
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			"context": {
				Cluster:   "default",
				AuthInfo:  "default",
				Namespace: namespace,
			},
		},
		CurrentContext: "context",
	}

	jsonConfig, err := runtime.Encode(clientcmdlatest.Codec, kubeconfigObject)
	if err != nil {
		return "", errors.New("unable to encode kubeconfig object")
	}

	yamlConfig, err := yaml.JSONToYAML(jsonConfig)
	if err != nil {
		return "", errors.Wrap(err, "unable to convert JSON to YAML")
	}

	return string(yamlConfig), nil
}

// GetDataKeyFromConfigMap returns the data key from a ConfigMap.
func GetDataKeyFromConfigMap(configMap *corev1.ConfigMap, key string) (string, error) {
	data, ok := configMap.Data[key]
	if !ok {
		return "", errors.Errorf("key %s not found in configmap %s", key, configMap.Name)
	}

	return data, nil
}

// // GetConfigMap returns a ConfigMap from the given namespaced name.
// func GetConfigMap(client client.Client, namespace string, name string) (*corev1.ConfigMap, error) {
// 	return client.CoreV1().ConfigMaps(namespace).Get(context.Background(), name, metav1.GetOptions{})
// }

// GetSerializedObjects returns an array of serialized objects from YAML string.
func GetSerializedObjects(yamlString string) ([]runtime.RawExtension, error) {
	decoder := machineryyaml.NewYAMLOrJSONDecoder(
		strings.NewReader(yamlString),
		readerBufferSize,
	)

	var objects []runtime.RawExtension

	for {
		var obj runtime.RawExtension

		err := decoder.Decode(&obj)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}

			return nil, err
		}

		objects = append(objects, obj)
	}

	return objects, nil
}

// GetSecrets returns a list of ConfigMaps from a list of serialized objects.
func GetSecrets(objects []runtime.RawExtension) ([]*corev1.Secret, []int, error) {
	secrets := []*corev1.Secret{}
	indexes := make([]int, 0, maxNumberOfSecrets)

	var secret *corev1.Secret

	for i, obj := range objects {
		secret = &corev1.Secret{}

		var unstructuredObj unstructured.Unstructured

		err := json.UnmarshalCaseSensitivePreserveInts(obj.Raw, &unstructuredObj)
		if err != nil {
			continue
		}

		if unstructuredObj.GetKind() != "Secret" || unstructuredObj.GetAPIVersion() != "v1" {
			continue
		}

		err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj.Object, secret)
		if err != nil {
			continue
		}

		indexes = append(indexes, i)
		secrets = append(secrets, secret)
	}

	return secrets, indexes, nil
}

// FindSecretByName returns a Secret from a list of Secret by name and namespace.
func FindSecretByName(secrets []*corev1.Secret, name string, namespace string) (*corev1.Secret, int, error) {
	for i, secret := range secrets {
		if secret.Name == name && secret.Namespace == namespace {
			return secret, i, nil
		}
	}

	return nil, 0, errors.Errorf("secret %s/%s not found in the configMap of the resourceSet", namespace, name)
}

// SetSecretData sets the data of a Secret.
func SetSecretData(secret *corev1.Secret, key string, value []byte) {
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}

	secret.Data[key] = value
}

// SetObjectByIndex sets an object in a list of serialized objects by index.
func SetObjectByIndex(objects []runtime.RawExtension, index int, obj runtime.RawExtension) {
	objects[index] = obj
}

// ModifyYAMlString modifies a YAML string by replacing the value of a key in a Secret.
func ModifyYAMlString(yamlString string, secretName string, secretNamespace string, key string, value []byte) (string, error) {
	objects, err := GetSerializedObjects(yamlString)
	if err != nil {
		return "", err
	}

	secrets, _, err := GetSecrets(objects)
	if err != nil {
		return "", err
	}

	secret, index, err := FindSecretByName(secrets, secretName, secretNamespace)
	if err != nil {
		// If the secret is not found (only possible reason for an error), create a new one
		secret = &corev1.Secret{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Secret",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: secretNamespace,
			},
			Type: corev1.SecretTypeOpaque,
		}
		index = len(objects) // Append the new secret at the end of the list
	}

	SetSecretData(secret, key, value)

	secretBytes, err := yaml.Marshal(secret)
	if err != nil {
		return "", err
	}

	// If the index is less than the length of the objects, we are updating an existing object
	if index < len(objects) {
		SetObjectByIndex(objects, index, runtime.RawExtension{
			Object: secret,
			Raw:    secretBytes,
		})
	}

	if index == len(objects) {
		// If the index is equal to the length of the objects, it means we are appending a new object
		objects = append(objects, runtime.RawExtension{
			Object: secret,
			Raw:    secretBytes,
		})
	}
	// Convert the objects back to YAML
	yamlString, err = serializeObjectsToYAML(objects)
	if err != nil {
		return "", err
	}

	return yamlString, nil
}

// serializeObjectsToYAML serializes a slice of runtime.RawExtension objects to a YAML string.
func serializeObjectsToYAML(objects []runtime.RawExtension) (string, error) {
	yamlStrings := []string{}

	for _, obj := range objects {
		yamlBytes, err := yaml.JSONToYAML(obj.Raw)
		if err != nil {
			return "", err
		}

		yamlStrings = append(yamlStrings, string(yamlBytes))
	}

	return strings.Join(yamlStrings, "\n---\n"), nil
}
