/*
Copyright 2024 The Kubernetes Authors.

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

package v1alpha1

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// EFSNamespaceInterface provides methods to work with EFSNamespace resources
type EFSNamespaceInterface interface {
	Namespace(namespace string) EFSNamespaceNamespaceInterface
	Create(ctx context.Context, efsNamespace *EFSNamespace, opts metav1.CreateOptions) (*EFSNamespace, error)
	Update(ctx context.Context, efsNamespace *EFSNamespace, opts metav1.UpdateOptions) (*EFSNamespace, error)
	UpdateStatus(ctx context.Context, efsNamespace *EFSNamespace, opts metav1.UpdateOptions) (*EFSNamespace, error)
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*EFSNamespace, error)
	List(ctx context.Context, opts metav1.ListOptions) (*EFSNamespaceList, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *EFSNamespace, err error)
}

// EFSNamespaceNamespaceInterface provides methods to work with namespaced EFSNamespace resources
type EFSNamespaceNamespaceInterface interface {
	Create(ctx context.Context, efsNamespace *EFSNamespace, opts metav1.CreateOptions) (*EFSNamespace, error)
	Update(ctx context.Context, efsNamespace *EFSNamespace, opts metav1.UpdateOptions) (*EFSNamespace, error)
	UpdateStatus(ctx context.Context, efsNamespace *EFSNamespace, opts metav1.UpdateOptions) (*EFSNamespace, error)
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*EFSNamespace, error)
	List(ctx context.Context, opts metav1.ListOptions) (*EFSNamespaceList, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *EFSNamespace, err error)
}

// EFSNamespaceClient implements EFSNamespaceInterface
type EFSNamespaceClient struct {
	restClient rest.Interface
	namespace  string
}

// NewEFSNamespaceClient creates a new client for EFSNamespace resources
func NewEFSNamespaceClient(config *rest.Config) (EFSNamespaceInterface, error) {
	// Register our types with the scheme
	if err := AddToScheme(scheme.Scheme); err != nil {
		return nil, err
	}

	// Create a REST client for our API group
	config.GroupVersion = &SchemeGroupVersion
	config.APIPath = "/apis"
	config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()

	client, err := rest.RESTClientFor(config)
	if err != nil {
		return nil, err
	}

	return &EFSNamespaceClient{
		restClient: client,
		namespace:  "", // Empty namespace for cluster-wide operations
	}, nil
}

// Namespace returns a namespace-scoped client
func (c *EFSNamespaceClient) Namespace(namespace string) EFSNamespaceNamespaceInterface {
	return &EFSNamespaceClient{
		restClient: c.restClient,
		namespace:  namespace,
	}
}

func (c *EFSNamespaceClient) Create(ctx context.Context, efsNamespace *EFSNamespace, opts metav1.CreateOptions) (*EFSNamespace, error) {
	result := &EFSNamespace{}
	req := c.restClient.Post().Resource("efsnamespaces")

	if c.namespace != "" {
		req = req.Namespace(c.namespace)
	}

	err := req.
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(efsNamespace).
		Do(ctx).
		Into(result)
	return result, err
}

func (c *EFSNamespaceClient) Update(ctx context.Context, efsNamespace *EFSNamespace, opts metav1.UpdateOptions) (*EFSNamespace, error) {
	result := &EFSNamespace{}
	req := c.restClient.Put().Resource("efsnamespaces").Name(efsNamespace.Name)

	if c.namespace != "" {
		req = req.Namespace(c.namespace)
	}

	err := req.
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(efsNamespace).
		Do(ctx).
		Into(result)
	return result, err
}

func (c *EFSNamespaceClient) UpdateStatus(ctx context.Context, efsNamespace *EFSNamespace, opts metav1.UpdateOptions) (*EFSNamespace, error) {
	result := &EFSNamespace{}
	req := c.restClient.Put().Resource("efsnamespaces").Name(efsNamespace.Name).SubResource("status")

	if c.namespace != "" {
		req = req.Namespace(c.namespace)
	}

	err := req.
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(efsNamespace).
		Do(ctx).
		Into(result)
	return result, err
}

func (c *EFSNamespaceClient) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	req := c.restClient.Delete().Resource("efsnamespaces").Name(name)

	if c.namespace != "" {
		req = req.Namespace(c.namespace)
	}

	return req.
		Body(&opts).
		Do(ctx).
		Error()
}

func (c *EFSNamespaceClient) Get(ctx context.Context, name string, opts metav1.GetOptions) (*EFSNamespace, error) {
	result := &EFSNamespace{}
	req := c.restClient.Get().Resource("efsnamespaces").Name(name)

	if c.namespace != "" {
		req = req.Namespace(c.namespace)
	}

	err := req.
		VersionedParams(&opts, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return result, err
}

func (c *EFSNamespaceClient) List(ctx context.Context, opts metav1.ListOptions) (*EFSNamespaceList, error) {
	result := &EFSNamespaceList{}
	req := c.restClient.Get().Resource("efsnamespaces")

	if c.namespace != "" {
		req = req.Namespace(c.namespace)
	}

	err := req.
		VersionedParams(&opts, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return result, err
}

func (c *EFSNamespaceClient) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	req := c.restClient.Get().Resource("efsnamespaces")

	if c.namespace != "" {
		req = req.Namespace(c.namespace)
	}

	return req.
		VersionedParams(&opts, scheme.ParameterCodec).
		Watch(ctx)
}

func (c *EFSNamespaceClient) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*EFSNamespace, error) {
	result := &EFSNamespace{}
	req := c.restClient.Patch(pt).Resource("efsnamespaces").Name(name).SubResource(subresources...)

	if c.namespace != "" {
		req = req.Namespace(c.namespace)
	}

	err := req.
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return result, err
}

// EFSNamespaceInformer provides informer functionality for EFSNamespace resources
type EFSNamespaceInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() EFSNamespaceLister
}

// EFSNamespaceLister helps list EFSNamespaces
type EFSNamespaceLister interface {
	List(selector labels.Selector) ([]*EFSNamespace, error)
	Get(name string) (*EFSNamespace, error)
}

// NewEFSNamespaceInformer creates a new informer for EFSNamespace resources
func NewEFSNamespaceInformer(client EFSNamespaceInterface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return client.List(context.TODO(), options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return client.Watch(context.TODO(), options)
			},
		},
		&EFSNamespace{},
		resyncPeriod,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)
}

// efsNamespaceLister implements the EFSNamespaceLister interface
type efsNamespaceLister struct {
	indexer cache.Indexer
}

// NewEFSNamespaceLister creates a new lister for EFSNamespace resources
func NewEFSNamespaceLister(indexer cache.Indexer) EFSNamespaceLister {
	return &efsNamespaceLister{indexer: indexer}
}

func (l *efsNamespaceLister) List(selector labels.Selector) ([]*EFSNamespace, error) {
	var ret []*EFSNamespace
	err := cache.ListAll(l.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*EFSNamespace))
	})
	return ret, err
}

func (l *efsNamespaceLister) Get(name string) (*EFSNamespace, error) {
	obj, exists, err := l.indexer.GetByKey(name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(Resource("efsnamespace"), name)
	}
	return obj.(*EFSNamespace), nil
}
