package kube

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/bundleadapter"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
)

type Client struct {
	host        string
	namespace   string
	bearerToken string
	httpClient  *http.Client
	adapter     bundleadapter.Adapter
}

type capabilityResponse struct {
	GPUProfiles         []string                     `json:"gpuProfiles"`
	RuntimeClasses      map[string]string            `json:"runtimeClasses"`
	NodeSelectors       map[string]map[string]string `json:"nodeSelectors"`
	DefaultNodeSelector map[string]string            `json:"defaultNodeSelector"`
}

func NewClient(config *Config) (*Client, error) {
	if config == nil {
		return nil, fmt.Errorf("kubernetes config is required")
	}
	if strings.TrimSpace(config.Host) == "" {
		return nil, fmt.Errorf("kubernetes host is required")
	}
	if strings.TrimSpace(config.Namespace) == "" {
		return nil, fmt.Errorf("kubernetes namespace is required")
	}
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{
		host:        strings.TrimRight(config.Host, "/"),
		namespace:   config.Namespace,
		bearerToken: strings.TrimSpace(config.BearerToken),
		httpClient:  httpClient,
		adapter:     bundleadapter.NewKubernetes(0),
	}, nil
}

func (c *Client) Upsert(ctx context.Context, object bundleadapter.Object) (bool, *bundleadapter.Object, error) {
	payload := object.Payload
	if len(payload) == 0 {
		return false, nil, fmt.Errorf("client object payload is required")
	}
	var previous *bundleadapter.Object
	for attempt := 0; attempt < 3; attempt++ {
		current, ok, err := c.Get(ctx, object)
		if err != nil {
			return false, nil, err
		}
		if !ok {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.host+c.adapter.CollectionPath(object, c.namespace), bytes.NewReader(payload))
			if err != nil {
				return false, nil, err
			}
			req.Header.Set("Content-Type", "application/json")
			c.applyAuth(req)
			resp, err := c.httpClient.Do(req)
			if err != nil {
				return false, nil, err
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusConflict {
				continue
			}
			if resp.StatusCode >= 300 {
				body, _ := io.ReadAll(resp.Body)
				return false, nil, fmt.Errorf("kubernetes create %s failed: %s: %s", object.Key, resp.Status, strings.TrimSpace(string(body)))
			}
			return true, nil, nil
		}
		previous = current
		updatedPayload, err := injectResourceVersion(payload, current.Version)
		if err != nil {
			return false, nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.host+c.adapter.ObjectPath(object, c.namespace), bytes.NewReader(updatedPayload))
		if err != nil {
			return false, nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		c.applyAuth(req)
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return false, nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusConflict {
			continue
		}
		if resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			return false, previous, fmt.Errorf("kubernetes update %s failed: %s: %s", object.Key, resp.Status, strings.TrimSpace(string(body)))
		}
		return false, previous, nil
	}
	if previous != nil {
		return false, previous, fmt.Errorf("kubernetes update conflict for %s: retry budget exhausted", object.Key)
	}
	return false, nil, fmt.Errorf("kubernetes create conflict for %s: retry budget exhausted", object.Key)
}

func (c *Client) EnsureRuntimeClass(ctx context.Context, object bundleadapter.Object) (bool, *bundleadapter.Object, error) {
	if object.Kind != "RuntimeClass" {
		return c.Upsert(ctx, object)
	}
	payload := object.Payload
	if len(payload) == 0 {
		return false, nil, fmt.Errorf("client object payload is required")
	}
	verifyExisting := func(previous *bundleadapter.Object) (bool, *bundleadapter.Object, error) {
		desiredHandler, err := runtimeClassHandler(payload)
		if err != nil {
			return false, previous, err
		}
		existingHandler, err := runtimeClassHandler(previous.Payload)
		if err != nil {
			return false, previous, err
		}
		if desiredHandler != existingHandler {
			return false, previous, fmt.Errorf("runtime class %s handler conflict: existing=%q desired=%q", object.Name, existingHandler, desiredHandler)
		}
		return false, previous, nil
	}
	previous, ok, err := c.Get(ctx, object)
	if err != nil {
		return false, nil, err
	}
	if !ok {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.host+c.adapter.CollectionPath(object, c.namespace), bytes.NewReader(payload))
		if err != nil {
			return false, nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		c.applyAuth(req)
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return false, nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusConflict {
			previous, found, err := c.Get(ctx, object)
			if err != nil {
				return false, nil, err
			}
			if !found {
				body, _ := io.ReadAll(resp.Body)
				return false, nil, fmt.Errorf("kubernetes create %s conflicted but runtime class was not readable: %s", object.Key, strings.TrimSpace(string(body)))
			}
			return verifyExisting(previous)
		}
		if resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			return false, nil, fmt.Errorf("kubernetes create %s failed: %s: %s", object.Key, resp.Status, strings.TrimSpace(string(body)))
		}
		return true, nil, nil
	}
	return verifyExisting(previous)
}

func (c *Client) Get(ctx context.Context, object bundleadapter.Object) (*bundleadapter.Object, bool, error) {
	body, statusCode, err := c.getURL(ctx, c.host+c.adapter.ObjectPath(object, c.namespace))
	if err != nil {
		return nil, false, err
	}
	if statusCode == http.StatusNotFound {
		return nil, false, nil
	}
	var envelope struct {
		Metadata struct {
			Generation      uint64 `json:"generation"`
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
		Spec any `json:"spec"`
	}
	_ = json.Unmarshal(body, &envelope)
	return &bundleadapter.Object{
		APIVersion: object.APIVersion,
		Kind:       object.Kind,
		Key:        object.Key,
		Name:       object.Name,
		Namespace:  object.Namespace,
		Generation: envelope.Metadata.Generation,
		Version:    envelope.Metadata.ResourceVersion,
		Payload:    body,
	}, true, nil
}

func (c *Client) GetPath(ctx context.Context, path string) ([]byte, error) {
	body, statusCode, err := c.getURL(ctx, c.host+path)
	if err == nil && statusCode == http.StatusNotFound {
		return nil, bundleadapter.ErrNotFound
	}
	return body, err
}

func (c *Client) ListBundles(ctx context.Context) ([]bundleadapter.Object, error) {
	prototype := bundleadapter.Object{APIVersion: "tgsrl.io/v1alpha1", Kind: "JobRunBundle", Namespace: c.namespace}
	body, statusCode, err := c.getURL(ctx, c.host+c.adapter.CollectionPath(prototype, c.namespace))
	if err != nil {
		return nil, err
	}
	if statusCode == http.StatusNotFound {
		return nil, nil
	}
	var list struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("decode jobrunbundle list: %w", err)
	}
	objects := make([]bundleadapter.Object, 0, len(list.Items))
	for _, item := range list.Items {
		var metadata struct {
			Metadata struct {
				Name            string `json:"name"`
				Namespace       string `json:"namespace"`
				Generation      uint64 `json:"generation"`
				ResourceVersion string `json:"resourceVersion"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(item, &metadata); err != nil {
			return nil, fmt.Errorf("decode jobrunbundle metadata: %w", err)
		}
		namespace := c.namespaceFor(metadata.Metadata.Namespace)
		objects = append(objects, bundleadapter.Object{APIVersion: "tgsrl.io/v1alpha1", Kind: "JobRunBundle", Key: namespace + "/" + metadata.Metadata.Name, Name: metadata.Metadata.Name, Namespace: namespace, Generation: metadata.Metadata.Generation, Version: metadata.Metadata.ResourceVersion, Payload: append([]byte(nil), item...)})
	}
	return objects, nil
}

func (c *Client) Delete(ctx context.Context, object bundleadapter.Object) (bool, error) {
	path := c.adapter.ObjectPath(object, c.namespace)
	payload, err := json.Marshal(map[string]any{"propagationPolicy": "Foreground"})
	if err != nil {
		return false, err
	}
	_, err = c.doJSON(ctx, http.MethodDelete, path, payload, "application/json")
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *Client) DiscoverCapabilities(ctx context.Context) (compiler.CapabilitySet, error) {
	body, statusCode, err := c.getURL(ctx, c.host+"/apis/tgsrl.io/v1alpha1/capabilities")
	if err != nil {
		return compiler.CapabilitySet{}, err
	}
	if statusCode == http.StatusNotFound {
		return c.discoverCapabilitiesByProbe(ctx)
	}
	var response capabilityResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return compiler.CapabilitySet{}, fmt.Errorf("decode capabilities: %w", err)
	}
	profiles := make(map[string]bool, len(response.GPUProfiles))
	for _, profile := range response.GPUProfiles {
		profiles[strings.TrimSpace(profile)] = true
	}
	if len(profiles) == 0 {
		profiles[compiler.GPUProfileNone] = true
	}
	return compiler.CapabilitySet{
		GPUProfiles:         profiles,
		RuntimeClasses:      response.RuntimeClasses,
		NodeSelectors:       response.NodeSelectors,
		DefaultNodeSelector: response.DefaultNodeSelector,
	}, nil
}

func (c *Client) discoverCapabilitiesByProbe(ctx context.Context) (compiler.CapabilitySet, error) {
	capabilities := compiler.DefaultCapabilitySet()

	runtimeClasses, err := c.discoverRuntimeClassesByProbe(ctx)
	if err != nil {
		return compiler.CapabilitySet{}, err
	}
	capabilities.RuntimeClasses = runtimeClasses

	gpuProfiles, err := c.discoverGPUProfilesByProbe(ctx)
	if err != nil {
		return compiler.CapabilitySet{}, err
	}
	for profile := range gpuProfiles {
		capabilities.GPUProfiles[profile] = true
	}

	return capabilities, nil
}

func (c *Client) discoverRuntimeClassesByProbe(ctx context.Context) (map[string]string, error) {
	body, statusCode, err := c.getURL(ctx, c.host+"/apis/node.k8s.io/v1/runtimeclasses")
	if err != nil {
		return nil, err
	}
	if statusCode == http.StatusNotFound {
		return nil, nil
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Handler string `json:"handler"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("decode runtime classes: %w", err)
	}
	values := make(map[string]string, len(list.Items))
	for _, item := range list.Items {
		name := strings.TrimSpace(item.Metadata.Name)
		if name == "" {
			continue
		}
		values[name] = strings.TrimSpace(item.Handler)
	}
	if len(values) == 0 {
		return nil, nil
	}
	return values, nil
}

func (c *Client) discoverGPUProfilesByProbe(ctx context.Context) (map[string]bool, error) {
	profiles := make(map[string]bool)

	body, statusCode, err := c.getURL(ctx, c.host+"/api/v1/nodes")
	if err != nil {
		return nil, err
	}
	if statusCode != http.StatusNotFound {
		var list struct {
			Items []struct {
				Status struct {
					Allocatable map[string]string `json:"allocatable"`
					Capacity    map[string]string `json:"capacity"`
				} `json:"status"`
			} `json:"items"`
		}
		if err := json.Unmarshal(body, &list); err != nil {
			return nil, fmt.Errorf("decode nodes: %w", err)
		}
		for _, item := range list.Items {
			if positiveResourceQuantity(item.Status.Allocatable["nvidia.com/gpu"]) || positiveResourceQuantity(item.Status.Capacity["nvidia.com/gpu"]) {
				profiles[compiler.GPUProfileNVIDIADevicePlugin] = true
			}
			if positiveResourceQuantity(item.Status.Allocatable["volcano.sh/gpu"]) || positiveResourceQuantity(item.Status.Capacity["volcano.sh/gpu"]) {
				profiles[compiler.GPUProfileVolcanoHAMI] = true
			}
		}
	}

	draSupported, err := c.discoverDRAGPUByProbe(ctx)
	if err != nil {
		return nil, err
	}
	if draSupported {
		profiles[compiler.GPUProfileKubernetesDRA] = true
	}
	if len(profiles) == 0 {
		return nil, nil
	}
	return profiles, nil
}

func (c *Client) discoverDRAGPUByProbe(ctx context.Context) (bool, error) {
	body, statusCode, err := c.getURL(ctx, c.host+"/apis/resource.k8s.io/v1beta1/deviceclasses")
	if err != nil {
		return false, err
	}
	if statusCode == http.StatusNotFound {
		draCollection := c.host + c.adapter.CollectionPath(bundleadapter.Object{
			APIVersion: "resource.k8s.io/v1beta1",
			Kind:       "ResourceClaim",
			Namespace:  c.namespace,
		}, c.namespace)
		_, statusCode, err = c.getURL(ctx, draCollection)
		if err != nil {
			return false, err
		}
		return statusCode != http.StatusNotFound, nil
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return false, fmt.Errorf("decode device classes: %w", err)
	}
	for _, item := range list.Items {
		name := strings.ToLower(strings.TrimSpace(item.Metadata.Name))
		if strings.Contains(name, "gpu") || strings.Contains(name, "nvidia") {
			return true, nil
		}
	}
	return len(list.Items) > 0, nil
}

func positiveResourceQuantity(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || value == "0" || value == "0m" {
		return false
	}
	numeric := value
	if index := strings.IndexFunc(value, func(r rune) bool { return !(r >= '0' && r <= '9') }); index > 0 {
		numeric = value[:index]
	}
	if numeric == "" {
		return true
	}
	parsed, err := strconv.ParseInt(numeric, 10, 64)
	if err != nil {
		return true
	}
	return parsed > 0
}

// ControlJob applies one Kubernetes lifecycle mutation. Pause/resume read back
// Job.spec.suspend immediately. Stop/terminate return the accepted deletion
// intent; the independent status observer owns terminal 404 convergence.
func (c *Client) ControlJob(ctx context.Context, object bundleadapter.Object, action tgsrlv1.JobCommandType, expectedVersion string) (*bundleadapter.JobControlReadback, error) {
	path := c.adapter.ObjectPath(object, c.namespace)
	switch action {
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME:
		suspend := action == tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE
		payload, err := json.Marshal(map[string]any{"metadata": map[string]string{"resourceVersion": expectedVersion}, "spec": map[string]bool{"suspend": suspend}})
		if err != nil {
			return nil, err
		}
		if _, err := c.doJSON(ctx, http.MethodPatch, path, payload, "application/merge-patch+json"); err != nil {
			return nil, err
		}
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP, tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE:
		payload, err := json.Marshal(map[string]any{"propagationPolicy": "Foreground", "preconditions": map[string]string{"resourceVersion": expectedVersion}})
		if err != nil {
			return nil, err
		}
		if _, err := c.doJSON(ctx, http.MethodDelete, path, payload, "application/json"); err != nil {
			return nil, err
		}
		return &bundleadapter.JobControlReadback{Deleting: true, ObservedAt: time.Now().UTC()}, nil
	default:
		return nil, fmt.Errorf("unsupported kubernetes job action %s", action)
	}
	body, statusCode, err := c.getURL(ctx, c.host+path)
	if err != nil {
		return nil, err
	}
	readback := &bundleadapter.JobControlReadback{ObservedAt: time.Now().UTC()}
	if statusCode == http.StatusNotFound {
		return nil, fmt.Errorf("kubernetes job %s disappeared during lifecycle readback", object.Key)
	}
	var job struct {
		Metadata struct {
			DeletionTimestamp string `json:"deletionTimestamp"`
		} `json:"metadata"`
		Spec struct {
			Suspend bool `json:"suspend"`
		} `json:"spec"`
		Status struct {
			Active    uint32 `json:"active"`
			Succeeded uint32 `json:"succeeded"`
			Failed    uint32 `json:"failed"`
		} `json:"status"`
	}
	if err := json.Unmarshal(body, &job); err != nil {
		return nil, fmt.Errorf("decode controlled job readback: %w", err)
	}
	readback.Paused = job.Spec.Suspend
	readback.Deleting = job.Metadata.DeletionTimestamp != ""
	readback.Active = job.Status.Active
	readback.Succeeded = job.Status.Succeeded
	readback.Failed = job.Status.Failed
	return readback, nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, payload []byte, contentType string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.host+path, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 && !(method == http.MethodDelete && resp.StatusCode == http.StatusNotFound) {
		return nil, fmt.Errorf("kubernetes %s %s failed: %s: %s", strings.ToLower(method), path, resp.Status, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func (c *Client) applyAuth(req *http.Request) {
	if c.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	}
}

func (c *Client) namespaceFor(value string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return c.namespace
}

func injectResourceVersion(payload []byte, resourceVersion string) ([]byte, error) {
	var object map[string]any
	if err := json.Unmarshal(payload, &object); err != nil {
		return nil, err
	}
	metadata, _ := object["metadata"].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
		object["metadata"] = metadata
	}
	metadata["resourceVersion"] = resourceVersion
	return json.Marshal(object)
}

func runtimeClassHandler(payload []byte) (string, error) {
	var object struct {
		Handler string `json:"handler"`
	}
	if err := json.Unmarshal(payload, &object); err != nil {
		return "", fmt.Errorf("decode runtime class: %w", err)
	}
	if strings.TrimSpace(object.Handler) == "" {
		return "", fmt.Errorf("runtime class handler is required")
	}
	return object.Handler, nil
}

func (c *Client) getURL(ctx context.Context, url string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		return nil, resp.StatusCode, fmt.Errorf("kubernetes get %s failed: %s: %s", url, resp.Status, strings.TrimSpace(string(body)))
	}
	return body, resp.StatusCode, nil
}
