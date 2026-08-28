package statuswatch

import (
	"context"
	"fmt"
	"time"

	"github.com/Blizzard-cyber/TGS-RL/operator-go/bundleadapter"
)

type KubernetesClient interface {
	GetPath(ctx context.Context, path string) ([]byte, error)
}

type KubernetesObserver struct {
	client  KubernetesClient
	adapter bundleadapter.Adapter
}

func NewKubernetes(client KubernetesClient, interval time.Duration) (*KubernetesObserver, error) {
	if client == nil {
		return nil, fmt.Errorf("kubernetes client is required")
	}
	return &KubernetesObserver{
		client:  client,
		adapter: bundleadapter.NewKubernetes(interval),
	}, nil
}

func (o *KubernetesObserver) Watch(ctx context.Context, request Request) (Stream, error) {
	if err := ValidateRequest(request); err != nil {
		return nil, err
	}
	clone, err := CloneRequest(request)
	if err != nil {
		return nil, err
	}
	stream, err := o.adapter.Watch(ctx, o.client, clone.Bundle)
	if err != nil {
		return nil, err
	}
	return &sliceStream{inner: stream}, nil
}
