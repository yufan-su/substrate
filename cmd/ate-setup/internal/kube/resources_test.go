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

package kube

import (
	"context"
	"maps"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// A default an operator has since edited must survive every later install, so
// an existing ConfigMap is left exactly as it is.
func TestCreateConfigMapIfAbsent(t *testing.T) {
	ctx := context.Background()
	edited := map[string]string{"policy.yaml": "edited"}
	c := &Client{Typed: kubefake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "existing"},
		Data:       edited,
	})}

	for _, tc := range []struct {
		name        string
		wantCreated bool
		wantData    map[string]string
	}{
		{name: "absent", wantCreated: true, wantData: map[string]string{"policy.yaml": "default"}},
		{name: "existing", wantCreated: false, wantData: edited},
	} {
		t.Run(tc.name, func(t *testing.T) {
			created, err := c.CreateConfigMapIfAbsent(ctx, "ate-system", tc.name, map[string]string{"policy.yaml": "default"})
			if err != nil {
				t.Fatalf("CreateConfigMapIfAbsent: %v", err)
			}
			if created != tc.wantCreated {
				t.Errorf("created = %v, want %v", created, tc.wantCreated)
			}
			cm, err := c.GetConfigMap(ctx, "ate-system", tc.name)
			if err != nil || cm == nil {
				t.Fatalf("GetConfigMap = %v, %v; want the ConfigMap", cm, err)
			}
			if !maps.Equal(cm.Data, tc.wantData) {
				t.Errorf("data = %v, want %v", cm.Data, tc.wantData)
			}
		})
	}
}
