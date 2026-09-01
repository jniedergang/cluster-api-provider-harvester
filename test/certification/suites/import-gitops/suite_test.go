//go:build e2e
// +build e2e

package importgitops

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	capiframework "sigs.k8s.io/cluster-api/test/framework"
	"sigs.k8s.io/cluster-api/test/framework/clusterctl"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/rancher/turtles/test/e2e"
	turtlesframework "github.com/rancher/turtles/test/framework"
	"github.com/rancher/turtles/test/testenv"

	"github.com/rancher-sandbox/cluster-api-provider-harvester/test/certification/suites"
)

var (
	ctx = context.Background()

	e2eConfig             *clusterctl.E2EConfig
	setupClusterResult    *testenv.SetupTestClusterResult
	bootstrapClusterProxy capiframework.ClusterProxy
	hostName              string

	// Stops the import watcher before teardown: its client calls would register
	// a ginkgo failure once the bootstrap kubeconfig is gone (recover alone
	// cannot undo a Fail already recorded).
	importWatcherStop     = make(chan struct{})
	importWatcherStopOnce sync.Once
)

func TestCAPHVImportGitops(t *testing.T) {
	RegisterFailHandler(Fail)

	ctrl.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	RunSpecs(t, "caphv-import-gitops")
}

// SynchronizedBeforeSuite stands up the full Rancher + Turtles management stack the
// Turtles integration suite expects (mirroring rancher/turtles-integration-suite-example)
// and installs the CAPHV provider on top. It runs with
// MANAGEMENT_CLUSTER_ENVIRONMENT=internal-kind: the kind cluster maps ports 80/443 onto
// the host so the Rancher server-url (RANCHER_HOSTNAME, e.g. <host-ip>.sslip.io) is
// reachable from the workload VMs provisioned on the real Harvester — a requirement for
// the cattle-cluster-agent import step that isolated-kind cannot satisfy.
var _ = SynchronizedBeforeSuite(
	func() []byte {
		e2eConfig = e2e.LoadE2EConfig()
		e2eConfig.ManagementClusterName = e2eConfig.ManagementClusterName + "-caphv-gitops"

		By("Setting up the management cluster (kind with host port mappings)")
		setupClusterResult = testenv.SetupTestCluster(ctx, testenv.SetupTestClusterInput{
			E2EConfig: e2eConfig,
			Scheme:    e2e.InitScheme(),
		})
		proxy := setupClusterResult.BootstrapClusterProxy

		// turtles#2641 standing trap: audit every API call touching the
		// management.cattle.io cluster records so that a record deleted by an
		// unidentified actor gets attributed. Injected after creation because
		// the CAPI kind provider exposes no kubeadm patch hook: editing the
		// static pod manifest makes the kubelet restart the API server with
		// audit logging enabled.
		if os.Getenv("CAPHV_DISABLE_APISERVER_AUDIT") != "true" {
			enableAPIServerAudit(setupClusterResult.ClusterName, proxy)
		}

		By("Deploying cert-manager")
		testenv.DeployCertManager(ctx, testenv.DeployCertManagerInput{
			BootstrapClusterProxy: proxy,
		})

		// The internal-kind ingress path of RancherDeployIngress expects a LoadBalancer
		// setup; on a bare kind the hostPort-based nginx manifest is the right fit (its
		// ports are exposed on the host through the kind extraPortMappings).
		By("Deploying the nginx ingress (hostPort)")
		Expect(turtlesframework.Apply(ctx, proxy, e2e.NginxIngress)).To(Succeed())
		waitForDeploymentAvailableOn(proxy, "ingress-nginx-controller", "ingress-nginx",
			e2eConfig.GetIntervals("default", "wait-controllers")...)

		// The controller Deployment turning Available does not mean the admission
		// webhook is servable yet; installing Rancher too early fails on
		// "failed calling webhook validate.nginx.ingress.kubernetes.io".
		By("Waiting for the nginx admission webhook endpoints")
		Eventually(func(g Gomega) {
			endpoints := &corev1.Endpoints{}
			g.Expect(proxy.GetClient().Get(ctx, types.NamespacedName{
				Namespace: "ingress-nginx", Name: "ingress-nginx-controller-admission",
			}, endpoints)).To(Succeed())
			g.Expect(endpoints.Subsets).ToNot(BeEmpty())
			g.Expect(endpoints.Subsets[0].Addresses).ToNot(BeEmpty())
		}, e2eConfig.GetIntervals("default", "wait-controllers")...).Should(Succeed())

		// Pods inside kind cannot reach the host IP that RANCHER_HOSTNAME resolves to
		// (hairpin through the podman port-forward times out), yet Turtles must download
		// the Rancher import manifest from the server-url. Point the hostname to the
		// kind node's internal IP in the cluster DNS: the node exposes nginx through
		// hostPorts, so in-cluster traffic reaches Rancher while external workload VMs
		// keep using the host-mapped ports.
		By("Resolving the Rancher hostname to the kind node inside the cluster DNS")
		nodeList := &corev1.NodeList{}
		Expect(proxy.GetClient().List(ctx, nodeList)).To(Succeed())
		Expect(nodeList.Items).ToNot(BeEmpty())
		var nodeIP string
		for _, addr := range nodeList.Items[0].Status.Addresses {
			if addr.Type == corev1.NodeInternalIP {
				nodeIP = addr.Address
			}
		}
		Expect(nodeIP).ToNot(BeEmpty(), "kind node must expose an internal IP")
		coreDNS := &corev1.ConfigMap{}
		Expect(proxy.GetClient().Get(ctx, types.NamespacedName{Namespace: "kube-system", Name: "coredns"}, coreDNS)).To(Succeed())
		hostsBlock := fmt.Sprintf("    hosts {\n       %s %s\n       fallthrough\n    }\n    forward", nodeIP, e2eConfig.GetVariableOrEmpty("RANCHER_HOSTNAME"))
		coreDNS.Data["Corefile"] = strings.Replace(coreDNS.Data["Corefile"], "    forward", hostsBlock, 1)
		Expect(proxy.GetClient().Update(ctx, coreDNS)).To(Succeed())
		Expect(proxy.GetClient().DeleteAllOf(ctx, &corev1.Pod{},
			client.InNamespace("kube-system"), client.MatchingLabels{"k8s-app": "kube-dns"})).To(Succeed())

		By("Deploying Rancher " + e2eConfig.GetVariableOrEmpty("RANCHER_VERSION"))
		rancherHookResult := testenv.DeployRancher(ctx, testenv.DeployRancherInput{
			BootstrapClusterProxy:   proxy,
			RancherIngressClassName: "nginx",
			RancherPatches:          [][]byte{e2e.RancherSettingPatch},
			RancherWaitInterval:     e2eConfig.GetIntervals("default", "wait-rancher"),
			ControllerWaitInterval:  e2eConfig.GetIntervals("default", "wait-controllers"),
		})

		// Rancher's system chart controller installs the released Turtles, which brings
		// up the CAPI core; the RKE2 providers and CAPHV are declared explicitly
		// (a fresh Rancher does not install them by itself).
		By("Waiting for the Rancher-managed Turtles + CAPI core to come up")
		for _, nn := range []struct{ name, namespace string }{
			{"rancher-turtles-controller-manager", e2e.NewRancherTurtlesNamespace},
			{"capi-controller-manager", "cattle-capi-system"},
		} {
			waitForDeploymentAvailableOn(proxy, nn.name, nn.namespace,
				e2eConfig.GetIntervals("default", "wait-rancher")...)
		}

		By("Deploying the RKE2 providers and the CAPHV " + e2eConfig.GetVariableOrEmpty("CAPHV_VERSION") + " CAPIProvider")
		for _, template := range [][]byte{suites.CAPIProviderRKE2Turtles, suites.CAPIProviderHarvesterTurtles} {
			Expect(turtlesframework.ApplyFromTemplate(ctx, turtlesframework.ApplyFromTemplateInput{
				Proxy:    proxy,
				Template: template,
			})).To(Succeed(), "Failed to apply CAPIProvider manifest")
		}

		By("Waiting for the provider deployments to be available")
		for _, nn := range []struct{ name, namespace string }{
			{"rke2-bootstrap-controller-manager", "rke2-bootstrap-system"},
			{"rke2-control-plane-controller-manager", "rke2-control-plane-system"},
			{"caphv-controller-manager", "caphv-system"},
		} {
			waitForDeploymentAvailableOn(proxy, nn.name, nn.namespace,
				e2eConfig.GetIntervals("default", "wait-controllers")...)
		}

		// Workaround for a Turtles 0.26 import race: if the freshly created v3 cluster
		// gets replaced, the CAPI Cluster is annotated imported=true before the agent
		// was ever applied and the import is never retried (the controller skips
		// clusters carrying the annotation). Strip it while premature; a genuinely
		// deleting cluster keeps it (the deletion flow relies on it).
		if os.Getenv("CAPHV_DISABLE_IMPORT_WORKAROUND") == "true" {
			By("turtles#2641 repro mode: the premature-import annotation watcher is DISABLED")
		}

		go func() {
			if os.Getenv("CAPHV_DISABLE_IMPORT_WORKAROUND") == "true" {
				return
			}
			for {
				select {
				case <-ctx.Done():
					return
				case <-importWatcherStop:
					return
				case <-time.After(30 * time.Second):
				}

				// Best-effort: once the suite tears the cluster down, the proxy's
				// accessors Fail (panic); swallow it instead of crashing the binary.
				func() {
					defer func() { _ = recover() }()

					clusters := &clusterv1.ClusterList{}
					if err := proxy.GetClient().List(ctx, clusters); err != nil {
						return
					}

					for i := range clusters.Items {
						cl := &clusters.Items[i]
						if cl.Annotations["imported"] != "true" || !cl.DeletionTimestamp.IsZero() {
							continue
						}

						delete(cl.Annotations, "imported")
						_ = proxy.GetClient().Update(ctx, cl)
					}
				}()
			}
		}()

		// turtles#2641 deterministic probe: delete the v3 cluster record once,
		// early in the import window (before the agent manifest exists), to
		// demonstrate what happens when a record vanishes mid-import. Whatever
		// deleted the record in the July occurrences, the question here is only
		// whether the import controller then poisons the live CAPI cluster with
		// imported=true (design flaw) or recovers by re-importing (theory wrong).
		induceDeletion := os.Getenv("CAPHV_INDUCE_RECORD_DELETION") == "true"
		if induceDeletion {
			By("turtles#2641 probe: will delete the first v3 cluster record once during the import window")
		}
		induced := false

		// turtles#2641 forensics: journal every management.cattle.io/v3 Cluster
		// record appearing or vanishing during the run, so a record replacement
		// is visible in the logs even when the import succeeds.
		go func() {
			known := map[string]string{}
			for {
				select {
				case <-ctx.Done():
					return
				case <-importWatcherStop:
					return
				case <-time.After(func() time.Duration {
					if induceDeletion && !induced {
						return time.Second // frapper avant l'application du manifest agent
					}
					return 5 * time.Second
				}()):
				}

				func() {
					defer func() { _ = recover() }()

					l := &unstructured.UnstructuredList{}
					l.SetGroupVersionKind(schema.GroupVersionKind{Group: "management.cattle.io", Version: "v3", Kind: "ClusterList"})
					if err := proxy.GetClient().List(ctx, l); err != nil {
						return
					}

					seen := map[string]bool{}
					for i := range l.Items {
						it := &l.Items[i]
						name := it.GetName()
						seen[name] = true
						display, _, _ := unstructured.NestedString(it.Object, "spec", "displayName")
						state := display
						if !it.GetDeletionTimestamp().IsZero() {
							state += " deleting"
						}
						if prev, ok := known[name]; !ok {
							GinkgoWriter.Printf("[v3-audit] %s record %s APPEARED (displayName=%q)\n", time.Now().UTC().Format(time.RFC3339), name, display)
							known[name] = state
							if induceDeletion && !induced && name != "local" {
								induced = true
								// Probe v3: hold the record in deleting state with an extra
								// finalizer, so reconciles observe it mid-deletion the way a
								// loaded Rancher does (minutes-long finalization in July).
								holders := append(it.GetFinalizers(), "caphv.probe/hold")
								it.SetFinalizers(holders)
								if err := proxy.GetClient().Update(ctx, it); err != nil {
									GinkgoWriter.Printf("[v3-audit] hold finalizer FAILED: %v\n", err)
								}
								GinkgoWriter.Printf("[v3-audit] %s INDUCING deletion of record %s (held by finalizer, turtles#2641 probe)\n", time.Now().UTC().Format(time.RFC3339), name)
								if err := proxy.GetClient().Delete(ctx, it); err != nil {
									GinkgoWriter.Printf("[v3-audit] induced deletion FAILED: %v\n", err)
								}
								// Release the hold after 90s so teardown never wedges.
								go func(recName string) {
									defer func() { _ = recover() }()
									time.Sleep(90 * time.Second)
									rec := &unstructured.Unstructured{}
									rec.SetGroupVersionKind(schema.GroupVersionKind{Group: "management.cattle.io", Version: "v3", Kind: "Cluster"})
									if err := proxy.GetClient().Get(ctx, client.ObjectKey{Name: recName}, rec); err != nil {
										return
									}
									kept := []string{}
									for _, f := range rec.GetFinalizers() {
										if f != "caphv.probe/hold" {
											kept = append(kept, f)
										}
									}
									rec.SetFinalizers(kept)
									_ = proxy.GetClient().Update(ctx, rec)
									GinkgoWriter.Printf("[v3-audit] %s hold finalizer released on %s\n", time.Now().UTC().Format(time.RFC3339), recName)
								}(name)
								// Force an immediate reconcile of the CAPI cluster so the
								// controller observes the record while it is still deleting
								// (finalizers keep it around for a few seconds): that is the
								// window in which reconcile annotates imported=true.
								capiClusters := &clusterv1.ClusterList{}
								if err := proxy.GetClient().List(ctx, capiClusters); err == nil {
									for j := range capiClusters.Items {
										cc := &capiClusters.Items[j]
										if cc.Namespace == "local" || !cc.DeletionTimestamp.IsZero() {
											continue
										}
										if cc.Annotations == nil {
											cc.Annotations = map[string]string{}
										}
										cc.Annotations["caphv-probe-poke"] = time.Now().UTC().Format(time.RFC3339)
										if err := proxy.GetClient().Update(ctx, cc); err == nil {
											GinkgoWriter.Printf("[v3-audit] poked CAPI cluster %s/%s to force a reconcile during the deletion window\n", cc.Namespace, cc.Name)
										}
									}
								}
							}
						} else if prev != state {
							GinkgoWriter.Printf("[v3-audit] %s record %s CHANGED (%q -> %q)\n", time.Now().UTC().Format(time.RFC3339), name, prev, state)
							known[name] = state
						}
					}
					for name := range known {
						if !seen[name] {
							GinkgoWriter.Printf("[v3-audit] %s record %s GONE\n", time.Now().UTC().Format(time.RFC3339), name)
							delete(known, name)
						}
					}
				}()
			}
		}()

		data, err := json.Marshal(e2e.Setup{
			ClusterName:     setupClusterResult.ClusterName,
			KubeconfigPath:  setupClusterResult.KubeconfigPath,
			RancherHostname: rancherHookResult.Hostname,
		})
		Expect(err).ToNot(HaveOccurred())

		return data
	},
	func(sharedData []byte) {
		setup := e2e.Setup{}
		Expect(json.Unmarshal(sharedData, &setup)).To(Succeed())

		hostName = setup.RancherHostname

		// LoadE2EConfig also exports the config variables into this process' environment.
		e2eConfig = e2e.LoadE2EConfig()

		bootstrapClusterProxy = capiframework.NewClusterProxy(
			setup.ClusterName,
			setup.KubeconfigPath,
			e2e.InitScheme(),
		)
		Expect(bootstrapClusterProxy).ToNot(BeNil(), "cluster proxy should not be nil")
	},
)

var _ = SynchronizedAfterSuite(
	func() {},
	func() {
		importWatcherStopOnce.Do(func() { close(importWatcherStop) })

		if setupClusterResult != nil && os.Getenv("CAPHV_DISABLE_APISERVER_AUDIT") != "true" {
			collectAPIServerAudit(setupClusterResult.ClusterName)
		}

		if bootstrapClusterProxy != nil {
			// Collects the management cluster state through crust-gather when the
			// kubectl plugin is installed (scripts/ensure-crust-gather.sh).
			By("Dumping artifacts from the bootstrap cluster")
			testenv.DumpBootstrapCluster(ctx, bootstrapClusterProxy.GetKubeconfigPath())
		}

		if setupClusterResult == nil {
			return
		}

		skipCleanup, _ := strconv.ParseBool(e2eConfig.GetVariableOrEmpty(e2e.SkipResourceCleanupVar))
		if skipCleanup {
			return
		}

		testenv.CleanupTestCluster(ctx, testenv.CleanupTestClusterInput{
			SetupTestClusterResult: *setupClusterResult,
		})
	},
)

// waitForDeploymentAvailableOn waits on the given proxy (usable before the package-level
// bootstrapClusterProxy is set in the all-processes setup).
func waitForDeploymentAvailableOn(proxy capiframework.ClusterProxy, name, namespace string, intervals ...interface{}) {
	capiframework.WaitForDeploymentsAvailable(ctx, capiframework.WaitForDeploymentsAvailableInput{
		Getter: proxy.GetClient(),
		Deployment: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		}},
	}, intervals...)
}

// enableAPIServerAudit enables API server audit logging scoped to the
// management.cattle.io cluster records inside the kind control-plane node.
// The whole-line substitutions keep kubeadm's indentation explicit, which is
// sturdier than sed append commands across GNU sed versions.
func enableAPIServerAudit(clusterName string, proxy capiframework.ClusterProxy) {
	node := clusterName + "-control-plane"

	By("Enabling API server audit logging for management.cattle.io cluster records on " + node)

	policy := `apiVersion: audit.k8s.io/v1
kind: Policy
rules:
- level: RequestResponse
  resources:
  - group: management.cattle.io
    resources: ["clusters"]
- level: None
`

	manifest := "/etc/kubernetes/manifests/kube-apiserver.yaml"
	steps := [][]string{
		{"docker", "exec", node, "mkdir", "-p", "/var/log/kubernetes"},
		{"docker", "exec", "-i", node, "cp", "/dev/stdin", "/etc/kubernetes/audit-policy.yaml"},
		{"docker", "exec", node, "sed", "-i",
			`s|^    - kube-apiserver$|    - kube-apiserver\n    - --audit-policy-file=/etc/kubernetes/audit-policy.yaml\n    - --audit-log-path=/var/log/kubernetes/audit.log\n    - --audit-log-maxsize=100\n    - --audit-log-maxbackup=1|`, manifest},
		{"docker", "exec", node, "sed", "-i",
			`s|^    volumeMounts:$|    volumeMounts:\n    - mountPath: /etc/kubernetes/audit-policy.yaml\n      name: audit-policy\n      readOnly: true\n    - mountPath: /var/log/kubernetes\n      name: audit-logs|`, manifest},
		{"docker", "exec", node, "sed", "-i",
			`s|^  volumes:$|  volumes:\n  - hostPath:\n      path: /etc/kubernetes/audit-policy.yaml\n      type: File\n    name: audit-policy\n  - hostPath:\n      path: /var/log/kubernetes\n      type: DirectoryOrCreate\n    name: audit-logs|`, manifest},
	}

	for i, args := range steps {
		cmd := exec.Command(args[0], args[1:]...)
		if i == 1 {
			cmd.Stdin = strings.NewReader(policy)
		}
		out, err := cmd.CombinedOutput()
		Expect(err).ToNot(HaveOccurred(), "audit setup step %v failed: %s", args, string(out))
	}

	// The kubelet restarts the API server with the new flags; wait for it to
	// come back before the suite piles work on it.
	Eventually(func(g Gomega) {
		nsList := &corev1.NamespaceList{}
		g.Expect(proxy.GetClient().List(ctx, nsList, client.Limit(1))).To(Succeed())
	}, "5m", "10s").Should(Succeed())
}

// collectAPIServerAudit saves the audit trail of the management.cattle.io
// cluster records into the artifacts folder before the kind cluster goes away.
func collectAPIServerAudit(clusterName string) {
	artifacts := os.Getenv("ARTIFACTS_FOLDER")
	if artifacts == "" {
		return
	}

	out, err := exec.Command("docker", "exec", clusterName+"-control-plane",
		"cat", "/var/log/kubernetes/audit.log").Output()
	if err != nil || len(out) == 0 {
		return
	}

	_ = os.WriteFile(filepath.Join(artifacts, "apiserver-audit-managementv3.log"), out, 0o600)
}
