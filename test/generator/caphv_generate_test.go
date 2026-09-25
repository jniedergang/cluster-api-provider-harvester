/*
Copyright 2026 SUSE.

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

// Package generator tests the bin/caphv-generate manifest generator offline: it renders
// the manifests with dummy flags (nothing is applied) and inspects the output.
package generator

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"

	infrav1 "github.com/harvester/cluster-api-provider-harvester/api/v1beta1"
)

const infraGroupPrefix = "infrastructure.cluster.x-k8s.io/"

// render runs the generator with the given extra flags and returns its YAML documents.
func render(t *testing.T, extraFlags ...string) [][]byte {
	t.Helper()

	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not available")
	}

	kubeconfig := filepath.Join(t.TempDir(), "harvester.yaml")

	err = os.WriteFile(kubeconfig, []byte("apiVersion: v1\nkind: Config\n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	args := append([]string{
		filepath.Join("..", "..", "bin", "caphv-generate"),
		"--name", "gen-test",
		"--image", "default/sles15-sp7-minimal-vm.x86_64-cloud-qu2.qcow2",
		"--ssh-keypair", "default/capi-ssh-key",
		"--network", "default/production",
		"--gateway", "172.16.0.1",
		"--subnet-mask", "255.255.0.0",
		"--harvester-kubeconfig", kubeconfig,
	}, extraFlags...)

	var stdout, stderr bytes.Buffer

	cmd := exec.CommandContext(t.Context(), bash, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		t.Fatalf("caphv-generate failed: %v\n%s", err, stderr.String())
	}

	var docs [][]byte

	reader := utilyaml.NewYAMLReader(bufio.NewReader(&stdout))

	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			t.Fatalf("unable to split the generator output: %v", err)
		}

		if len(bytes.TrimSpace(doc)) > 0 {
			docs = append(docs, doc)
		}
	}

	return docs
}

// collectInfraAPIVersions returns every apiVersion of the infrastructure group found in
// an object, at any depth (object apiVersion, ClusterClass template references and patch
// selectors alike).
func collectInfraAPIVersions(node any, found *[]string) {
	switch value := node.(type) {
	case map[string]any:
		for key, child := range value {
			if s, ok := child.(string); ok && key == "apiVersion" && strings.HasPrefix(s, infraGroupPrefix) {
				*found = append(*found, s)
			}

			collectInfraAPIVersions(child, found)
		}
	case []any:
		for _, child := range value {
			collectInfraAPIVersions(child, found)
		}
	}
}

// TestGeneratorEmitsV1beta1 guards against the generator falling back to the deprecated
// v1alpha1 API: objects created through v1alpha1 are converted on write, and the cloned
// templates were then rejected by the CAPI topology controller.
func TestGeneratorEmitsV1beta1(t *testing.T) {
	cases := map[string][]string{
		"CRS mode, single IP pool": {"--ip-pool", "capi-vm-pool"},
		"Fleet mode, pool list and extra disk": {
			"--ip-pool-refs", "pool-a,pool-b",
			"--extra-disk", "10Gi:longhorn",
			"--dns", "172.16.3.6,8.8.8.8",
			"--fleet-addon-repo", "https://git.example/caphv-fleet-addons.git",
		},
	}

	for name, flags := range cases {
		t.Run(name, func(t *testing.T) {
			docs := render(t, flags...)

			var apiVersions []string

			kinds := map[string]int{}

			for _, doc := range docs {
				if bytes.Contains(doc, []byte(infraGroupPrefix+"v1alpha1")) {
					t.Errorf("the generator still emits the deprecated v1alpha1 API:\n%s", doc)
				}

				obj := map[string]any{}

				err := yaml.Unmarshal(doc, &obj)
				if err != nil {
					t.Fatalf("invalid YAML document: %v\n%s", err, doc)
				}

				collectInfraAPIVersions(obj, &apiVersions)

				kind, _ := obj["kind"].(string)
				kinds[kind]++

				// The v1beta1 types must accept every field the generator writes.
				var typed any

				switch kind {
				case "HarvesterClusterTemplate":
					typed = &infrav1.HarvesterClusterTemplate{}
				case "HarvesterMachineTemplate":
					typed = &infrav1.HarvesterMachineTemplate{}
				}

				if typed != nil {
					err = yaml.UnmarshalStrict(doc, typed)
					if err != nil {
						t.Errorf("%s does not match the v1beta1 schema: %v", kind, err)
					}
				}
			}

			// One HarvesterClusterTemplate, two HarvesterMachineTemplates (control plane and
			// workers), and the ClusterClass referencing them: a vacuous pass is not a pass.
			if kinds["HarvesterClusterTemplate"] != 1 || kinds["HarvesterMachineTemplate"] != 2 || kinds["ClusterClass"] != 1 {
				t.Fatalf("unexpected generator output, kinds: %v", kinds)
			}

			// 3 template objects + 3 ClusterClass references + the patch selectors.
			if len(apiVersions) < 6 {
				t.Fatalf("expected the templates and ClusterClass references in the output, found %d apiVersions", len(apiVersions))
			}

			want := infrav1.GroupVersion.String()
			for _, apiVersion := range apiVersions {
				if apiVersion != want {
					t.Errorf("infrastructure apiVersion %q, want %q", apiVersion, want)
				}
			}
		})
	}
}
