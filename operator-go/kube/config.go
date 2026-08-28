package kube

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const (
	defaultNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	defaultTokenPath     = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	defaultCAPath        = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

type Config struct {
	Host        string
	BearerToken string
	Namespace   string
	HTTPClient  *http.Client
}

func LoadConfig(kubeconfigPath, namespace string) (*Config, error) {
	if kubeconfigPath != "" {
		return loadKubeconfig(kubeconfigPath, namespace)
	}
	return loadInCluster(namespace)
}

func loadInCluster(namespace string) (*Config, error) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	if host == "" || port == "" {
		return nil, fmt.Errorf("kubernetes in-cluster env is not set")
	}
	token, err := os.ReadFile(defaultTokenPath)
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	if namespace == "" {
		rawNamespace, err := os.ReadFile(defaultNamespacePath)
		if err != nil {
			return nil, fmt.Errorf("read service account namespace: %w", err)
		}
		namespace = strings.TrimSpace(string(rawNamespace))
	}
	caPool := x509.NewCertPool()
	if caBytes, err := os.ReadFile(defaultCAPath); err == nil {
		caPool.AppendCertsFromPEM(caBytes)
	}
	return &Config{
		Host:        "https://" + host + ":" + port,
		BearerToken: strings.TrimSpace(string(token)),
		Namespace:   namespace,
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: caPool},
			},
		},
	}, nil
}

func loadKubeconfig(path, namespace string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open kubeconfig: %w", err)
	}
	defer file.Close()

	var (
		server    string
		token     string
		caDataB64 string
		currentNS string
	)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case strings.HasPrefix(line, "server:"):
			server = strings.TrimSpace(strings.TrimPrefix(line, "server:"))
		case strings.HasPrefix(line, "token:"):
			token = strings.TrimSpace(strings.TrimPrefix(line, "token:"))
		case strings.HasPrefix(line, "certificate-authority-data:"):
			caDataB64 = strings.TrimSpace(strings.TrimPrefix(line, "certificate-authority-data:"))
		case strings.HasPrefix(line, "namespace:"):
			currentNS = strings.TrimSpace(strings.TrimPrefix(line, "namespace:"))
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan kubeconfig: %w", err)
	}
	if namespace == "" {
		namespace = currentNS
	}
	if server == "" {
		return nil, fmt.Errorf("kubeconfig server is required")
	}
	caPool := x509.NewCertPool()
	if caDataB64 != "" {
		caBytes, err := base64.StdEncoding.DecodeString(caDataB64)
		if err != nil {
			return nil, fmt.Errorf("decode kubeconfig certificate-authority-data: %w", err)
		}
		caPool.AppendCertsFromPEM(caBytes)
	}
	return &Config{
		Host:        strings.TrimRight(server, "/"),
		BearerToken: token,
		Namespace:   namespace,
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: caPool},
			},
		},
	}, nil
}

func ResolveKubeconfig(explicitPath string) string {
	if explicitPath != "" {
		return explicitPath
	}
	if env := strings.TrimSpace(os.Getenv("KUBECONFIG")); env != "" {
		if _, err := os.Stat(env); err == nil {
			return env
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	path := filepath.Join(home, ".kube", "config")
	if _, err := os.Stat(path); err == nil {
		return path
	}
	return ""
}
