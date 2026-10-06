// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
	"github.com/agent-substrate/substrate/tools/egress-tests/internal/targetcert"
)

// What deploy.sh --patch-gateway creates so the egress gateway trusts the
// target's certificate when it re-originates TLS. Keep in sync with deploy.sh
// and manifests/gateway-trust-patch.yaml.tmpl.
const (
	gatewayDeployment = "atenet-egress"
	// gatewayTrustInitContainer merges the target's CA into the roots Envoy
	// verifies upstream certificates against.
	gatewayTrustInitContainer = "egress-tests-trust"
	// gatewayCAConfigMap holds the target's CA, under targetcert.CAFile.
	gatewayCAConfigMap = "egress-tests-target-ca"
	// gatewayCAAnnotation on the gateway's pod template is the SHA-256 of
	// gatewayCAConfigMap's CA, so a new CA rolls the gateway.
	gatewayCAAnnotation = "egress-tests.ate.dev/target-ca-sha256"
	// targetTLSSecret holds the target's certificate, key and CA.
	targetTLSSecret = "egress-target-tls"
)

const patchHint = "run tools/egress-tests/deploy.sh --patch-gateway"

// checkGatewayTrust fails unless the egress gateway trusts the CA that signed
// the target's certificate and runs with that trust. Without it, the gateway
// cannot verify the target and every HTTPS request gets a 503.
func checkGatewayTrust(ctx context.Context, k8s kubernetes.Interface) error {
	secret, err := k8s.CoreV1().Secrets(egressapi.TargetNamespace).Get(ctx, targetTLSSecret, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("reading the target's TLS Secret %s/%s: %w; run tools/egress-tests/deploy.sh --deploy --https", egressapi.TargetNamespace, targetTLSSecret, err)
	}
	ns := installdefaults.SystemNamespace
	cm, err := k8s.CoreV1().ConfigMaps(ns).Get(ctx, gatewayCAConfigMap, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("the egress gateway is not set up to trust the target (ConfigMap %s/%s: %w); %s", ns, gatewayCAConfigMap, err, patchHint)
	}
	gatewayCA := []byte(cm.Data[targetcert.CAFile])
	if !sameCertificate(secret.Data[targetcert.CAFile], gatewayCA) {
		return fmt.Errorf("the CA the egress gateway trusts did not sign the target's certificate; %s", patchHint)
	}

	dep, err := k8s.AppsV1().Deployments(ns).Get(ctx, gatewayDeployment, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("reading the egress gateway Deployment %s/%s: %w", ns, gatewayDeployment, err)
	}
	if !slices.ContainsFunc(dep.Spec.Template.Spec.InitContainers, func(c corev1.Container) bool { return c.Name == gatewayTrustInitContainer }) {
		return fmt.Errorf("the egress gateway is not patched to trust the target's CA; %s", patchHint)
	}
	sum := sha256.Sum256(gatewayCA)
	if dep.Spec.Template.Annotations[gatewayCAAnnotation] != hex.EncodeToString(sum[:]) {
		return fmt.Errorf("the egress gateway was patched for a different CA than the one it should trust; %s", patchHint)
	}
	want := int32(1)
	if dep.Spec.Replicas != nil {
		want = *dep.Spec.Replicas
	}
	st := dep.Status
	if st.ObservedGeneration < dep.Generation || st.UpdatedReplicas != want || st.AvailableReplicas != want || st.Replicas != want {
		return fmt.Errorf("the egress gateway is still rolling out its trust patch; wait for `kubectl -n %s rollout status deploy/%s`", ns, gatewayDeployment)
	}
	return nil
}

// sameCertificate reports whether a and b are PEM encodings of the same
// certificate.
func sameCertificate(a, b []byte) bool {
	da, errA := certificateDER(a)
	db, errB := certificateDER(b)
	return errA == nil && errB == nil && bytes.Equal(da, db)
}

func certificateDER(data []byte) ([]byte, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("not a PEM certificate")
	}
	return block.Bytes, nil
}
