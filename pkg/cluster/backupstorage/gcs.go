// Copyright 2017 The etcd-operator Authors
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

package backupstorage

import "github.com/coreos/etcd-operator/pkg/spec"

import (
	"context"
	"path/filepath"

	backupgcs "github.com/coreos/etcd-operator/pkg/backup/gcs"
	"github.com/coreos/etcd-operator/pkg/backup/gcs/gcsconfig"
	"github.com/coreos/etcd-operator/pkg/spec"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	gcpConfigDirPrefix = "/etc/etcd-operator/gcp/"
)

type gcs struct {
	gcsconfig.GCSContext
	clusterName  string
	namespace    string
	backupPolicy spec.BackupPolicy
	kubecli      kubernetes.Interface
	gcscli       *backupgcs.GCS
}

// NewGCSStorage returns a new Storage backed by Google Cloud Storage.
func NewGCSStorage(gcsCtx gcsconfig.GCSContext, kubecli kubernetes.Interface, clusterName, ns string, p spec.BackupPolicy) (Storage, error) {
	prefix := filepath.Join(ns, clusterName)
	cli, err := func() (*backupgcs.GCS, error) {
		if p.GCS != nil {
			key, err := getGCSKey(kubecli, ns, p.GCS.GCSKey)
			if err != nil {
				return nil, err
			}
			return backupgcs.New(context.Background(), p.GCS.GCSBucket, p.GCS.GCSScope, key, prefix)
		}
		return backupgcs.New(context.Background(), gcsCtx.Bucket, gcsCtx.Scope, gcsCtx.JSONKey, prefix)
	}()
	if err != nil {
		return nil, err
	}

	g := &gcs{
		GCSContext:   gcsCtx,
		kubecli:      kubecli,
		clusterName:  clusterName,
		backupPolicy: p,
		namespace:    ns,
		gcscli:       cli,
	}
	return g, nil
}

// Create is no-op. gcs.New already ensures the bucket exist.
func (g *gcs) Create() error { return nil }

func (g *gcs) Clone(from string) error {
	prefix := g.namespace + "/" + from
	return g.gcscli.CopyPrefix(prefix)
}

func (g *gcs) Delete() error {
	if g.backupPolicy.CleanupBackupsOnClusterDelete {
		names, err := g.gcscli.List()
		if err != nil {
			return err
		}
		for _, n := range names {
			err = g.gcscli.Delete(n)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func getGCSKey(kubecli kubernetes.Interface, ns, secret string) ([]byte, error) {
	se, err := kubecli.CoreV1().Secrets(ns).Get(secret, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	return []byte(se.Data[spec.GCSJSONKeyFileName]), nil
}
