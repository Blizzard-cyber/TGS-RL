#!/usr/bin/env ruby

require "open3"
require "yaml"

CHART_DIR = File.expand_path("..", __dir__)
REPO_ROOT = File.expand_path("../../..", CHART_DIR)
RAW_MANIFEST = File.join(REPO_ROOT, "deploy/kubernetes/operator.yaml")
EXTERNAL_CRD_MANIFEST = File.join(REPO_ROOT, "deploy/crds/tgsrl_jobrunbundles.yaml")
OPERATOR_DOCKERFILE = File.join(REPO_ROOT, "Dockerfile.operator")
HELM_RELEASE = "contract-test"
HELM_NAMESPACE = "tgsrl-system"
OVERRIDE_REPOSITORY = "registry.example.test/tgsrl/operator"
OVERRIDE_TAG = "9.9.9"
OVERRIDE_DIGEST = "sha256:" + ("1" * 64)

def assert(condition, message)
  raise message unless condition
end

def documents(yaml)
  YAML.load_stream(yaml).compact
end

def one(docs, kind)
  matches = docs.select { |doc| doc["kind"] == kind }
  assert(matches.length == 1, "expected one #{kind}, found #{matches.length}")
  matches.first
end

def maybe_one(docs, kind)
  matches = docs.select { |doc| doc["kind"] == kind }
  assert(matches.length <= 1, "expected at most one #{kind}, found #{matches.length}")
  matches.first
end

def cluster_rbac_for(docs, resource, verb = nil)
  role = docs.select { |doc| doc["kind"] == "ClusterRole" }.find do |candidate|
    candidate.fetch("rules", []).any? do |rule|
      rule.fetch("resources", []).include?(resource) && (verb.nil? || rule.fetch("verbs", []).include?(verb))
    end
  end
  assert(!role.nil?, "missing ClusterRole for #{resource}")
  binding = docs.select { |doc| doc["kind"] == "ClusterRoleBinding" }.find do |candidate|
    candidate.dig("roleRef", "name") == role.dig("metadata", "name")
  end
  assert(!binding.nil?, "missing ClusterRoleBinding for #{resource}")
  [role, binding]
end

def assert_discovery_rbac(role, binding, service_account_name, expected_namespace)
  expected = {
    ["", "nodes"] => ["list"],
    ["node.k8s.io", "runtimeclasses"] => ["list"],
    ["resource.k8s.io", "deviceclasses"] => ["list"]
  }
  actual = role.fetch("rules").to_h do |rule|
    [[rule.fetch("apiGroups").first, rule.fetch("resources").first], rule.fetch("verbs")]
  end
  assert(actual == expected, "discovery ClusterRole exceeds or misses the read contract")
  assert(binding.dig("roleRef", "name") == role.dig("metadata", "name"), "discovery binding targets the wrong role")
  subject = binding.fetch("subjects").first
  assert(subject["name"] == service_account_name, "discovery binding targets the wrong service account")
  assert(subject["namespace"] == expected_namespace, "discovery binding targets the wrong namespace")
end

def assert_namespaced_rbac(role, binding, expected_namespace)
  assert(role.fetch("metadata", {}).fetch("namespace", nil) == expected_namespace, "Role must be scoped to the target namespace")
  assert(binding.fetch("metadata", {}).fetch("namespace", nil) == expected_namespace, "RoleBinding must be scoped to the target namespace")
  assert(binding.dig("roleRef", "kind") == "Role", "RoleBinding must target a namespaced Role")
  subject = binding.fetch("subjects").first
  assert(subject["kind"] == "ServiceAccount", "RoleBinding subject must be the operator ServiceAccount")
  assert(subject["namespace"] == expected_namespace, "RoleBinding subject must stay in the target namespace")

  bundle_rule = role.fetch("rules").find do |rule|
    rule["apiGroups"] == ["tgsrl.io"] && rule["resources"] == ["jobrunbundles"]
  end
  assert(!bundle_rule.nil?, "missing dedicated jobrunbundles RBAC rule")
  assert(
    bundle_rule["verbs"] == %w[get list watch create update patch],
    "jobrunbundles RBAC verbs do not match the operator read/upsert contract"
  )

  status_rule = role.fetch("rules").find do |rule|
    rule["apiGroups"] == ["tgsrl.io"] && rule["resources"] == ["jobrunbundles/status"]
  end
  assert(!status_rule.nil?, "missing dedicated jobrunbundles/status RBAC rule")
  assert(status_rule["verbs"] == %w[get update patch], "status subresource must not receive create")

  runtimeclass_rule = role.fetch("rules").find do |rule|
    rule["apiGroups"] == ["node.k8s.io"] && rule["resources"] == ["runtimeclasses"]
  end
  assert(runtimeclass_rule.nil?, "default namespaced RBAC must not include RuntimeClass write access")
end

def assert_runtimeclass_rbac(cluster_role, cluster_binding, service_account_name, expected_namespace)
  assert(!cluster_role.nil?, "runtimeClassCreate=true must render a dedicated ClusterRole")
  assert(!cluster_binding.nil?, "runtimeClassCreate=true must render a dedicated ClusterRoleBinding")
  runtimeclass_rule = cluster_role.fetch("rules").find do |rule|
    rule["apiGroups"] == ["node.k8s.io"] && rule["resources"] == ["runtimeclasses"]
  end
  assert(!runtimeclass_rule.nil?, "dedicated ClusterRole must isolate runtimeclasses")
  assert(
    runtimeclass_rule["verbs"] == %w[get create],
    "runtimeclasses ClusterRole must match the create-or-verify contract"
  )
  cluster_role_name = cluster_role.dig("metadata", "name")
  assert(cluster_role_name.length <= 63, "RuntimeClass ClusterRole name exceeds the DNS label limit")
  assert(cluster_role_name.match?(/\A[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\z/), "RuntimeClass ClusterRole name is not a DNS label")
  assert(cluster_binding.dig("metadata", "name") == cluster_role_name, "RuntimeClass ClusterRole and binding names must match")
  assert(cluster_binding.dig("roleRef", "kind") == "ClusterRole", "ClusterRoleBinding must target the dedicated ClusterRole")
  assert(cluster_binding.dig("roleRef", "name") == cluster_role.dig("metadata", "name"), "ClusterRoleBinding must reference the dedicated ClusterRole by name")
  subject = cluster_binding.fetch("subjects").first
  assert(subject["kind"] == "ServiceAccount", "ClusterRoleBinding subject must be the operator ServiceAccount")
  assert(subject["name"] == service_account_name, "ClusterRoleBinding subject name must match the operator ServiceAccount")
  assert(subject["namespace"] == expected_namespace, "ClusterRoleBinding subject namespace must match the operator namespace")
end

def assert_workload_contract(docs, expected_claim)
  deployment = one(docs, "Deployment")
  service = one(docs, "Service")
  container = deployment.dig("spec", "template", "spec", "containers").first
  args = container.fetch("args")

  assert(deployment.dig("spec", "replicas") == 1, "operator must remain a singleton")
  assert(deployment.dig("spec", "strategy", "type") == "Recreate", "RWO state requires Recreate strategy")
  pod_security_context = deployment.dig("spec", "template", "spec", "securityContext")
  assert(pod_security_context["runAsNonRoot"] == true, "operator pod must run as non-root")
  assert(pod_security_context["runAsUser"] == 65_532, "operator pod must use the distroless non-root UID")
  assert(pod_security_context["runAsGroup"] == 65_532, "operator pod must use the distroless non-root GID")
  assert(pod_security_context["fsGroup"] == 65_532, "cursor volume must be writable by the operator group")
  assert(pod_security_context["fsGroupChangePolicy"] == "OnRootMismatch", "cursor volume ownership changes must be bounded")
  assert(pod_security_context.dig("seccompProfile", "type") == "RuntimeDefault", "operator pod must use RuntimeDefault seccomp")

  security_context = container.fetch("securityContext")
  assert(security_context["runAsNonRoot"] == true, "operator container must run as non-root")
  assert(security_context["allowPrivilegeEscalation"] == false, "operator container must forbid privilege escalation")
  assert(security_context["readOnlyRootFilesystem"] == true, "operator container root filesystem must be read-only")
  assert(security_context.dig("capabilities", "drop") == ["ALL"], "operator container must drop all Linux capabilities")
  %w[
    --scheduler=tgsrl-scheduler:50051
    --control=tgsrl-job-controller:50061
    --runtime=tgsrl-runtime:50071
    --listen=0.0.0.0:50081
    --namespace=tgsrl-system
    --gpu-profile=none
    --cursor-dir=/var/lib/tgsrl-operator
  ].each { |arg| assert(args.include?(arg), "missing operator argument #{arg}") }

  port = container.fetch("ports").find { |candidate| candidate["name"] == "grpc" }
  assert(port && port["containerPort"] == 50_081, "operator gRPC container port must be 50081")
  mount = container.fetch("volumeMounts").find { |candidate| candidate["name"] == "cursor-state" }
  assert(mount && mount["mountPath"] == "/var/lib/tgsrl-operator", "cursor arg and mount path differ")

  volume = deployment.dig("spec", "template", "spec", "volumes").find do |candidate|
    candidate["name"] == "cursor-state"
  end
  if expected_claim
    assert(volume.dig("persistentVolumeClaim", "claimName") == expected_claim, "Deployment references the wrong PVC")
  else
    assert(volume["emptyDir"] == {}, "persistence-disabled deployment must keep cursor-dir writable with emptyDir")
  end

  service_port = service.dig("spec", "ports").find { |candidate| candidate["name"] == "grpc" }
  assert(service_port["port"] == port["containerPort"], "Service and container ports differ")
  assert(service_port["targetPort"] == "grpc", "Service must target the named container port")
end

def assert_image_reference(docs, expected)
  deployment = one(docs, "Deployment")
  container = deployment.dig("spec", "template", "spec", "containers").first
  assert(container.fetch("image") == expected, "rendered operator image does not match expected reference")
end

def run_chart(release, namespace, *extra_args)
  Open3.capture2e(
    "helm",
    "template",
    release,
    CHART_DIR,
    "--namespace",
    namespace,
    *extra_args
  )
end

def render_chart_for(release, namespace, *extra_args)
  rendered, status = run_chart(release, namespace, *extra_args)
  assert(status.success?, "helm template failed:\n#{rendered}")
  documents(rendered)
end

def render_chart(*extra_args)
  render_chart_for(HELM_RELEASE, HELM_NAMESPACE, *extra_args)
end

raw_docs = documents(File.read(RAW_MANIFEST))
assert(raw_docs.none? { |doc| doc["kind"] == "CustomResourceDefinition" }, "raw operator manifest must not own the external CRD")
assert(one(raw_docs, "Namespace").dig("metadata", "name") == "tgsrl-system", "raw install manifest must create the target namespace")
assert_namespaced_rbac(one(raw_docs, "Role"), one(raw_docs, "RoleBinding"), "tgsrl-system")
raw_discovery_role, raw_discovery_binding = cluster_rbac_for(raw_docs, "nodes")
assert_discovery_rbac(raw_discovery_role, raw_discovery_binding, "tgsrl-operator", "tgsrl-system")
assert_workload_contract(raw_docs, "tgsrl-operator-state")
raw_pvc = one(raw_docs, "PersistentVolumeClaim")
assert(raw_pvc.dig("spec", "accessModes") == ["ReadWriteOnce"], "raw PVC must default to ReadWriteOnce")
assert(raw_pvc.dig("spec", "resources", "requests", "storage") == "1Gi", "raw PVC must request 1Gi")

crd = one(documents(File.read(EXTERNAL_CRD_MANIFEST)), "CustomResourceDefinition")
assert(crd.dig("metadata", "name") == "jobrunbundles.tgsrl.io", "external CRD prerequisite has the wrong name")
version = crd.dig("spec", "versions").find { |candidate| candidate["name"] == "v1alpha1" }
assert(version.dig("subresources", "status") == {}, "CRD must expose the status subresource granted by RBAC")
spec_schema = version.dig("schema", "openAPIV3Schema", "properties", "spec")
assert(spec_schema["required"] == ["bundle"], "CRD spec must require the adapter's single bundle envelope")
assert(spec_schema.fetch("properties").keys == ["bundle"], "CRD must not duplicate bundle identity fields")
bundle_schema = spec_schema.dig("properties", "bundle")
assert(bundle_schema["type"] == "object", "CRD spec.bundle must be an object")
assert(bundle_schema["x-kubernetes-preserve-unknown-fields"] == true, "API server must preserve the evolving bundle payload")

values = YAML.load_file(File.join(CHART_DIR, "values.yaml"))
assert(values.dig("image", "digest") == "", "local chart default must leave digest unset")
assert(values.dig("persistence", "enabled") == true, "durable state must be enabled by default")
assert(values.dig("persistence", "retain") == true, "chart-managed state must be retained by default")
assert(values.dig("persistence", "existingClaim") == "", "managed PVC must be the default")
assert(values.dig("persistence", "accessModes") == ["ReadWriteOnce"], "Helm PVC must default to ReadWriteOnce")
assert(values.dig("controller", "gpuProfile") == "none", "GPU profile must default to none")
assert(values.dig("controller", "runtimeClassName") == "", "runtime class name must default to empty")
assert(values.dig("controller", "runtimeClassHandler") == "", "runtime class handler must default to empty")
assert(values.dig("controller", "runtimeClassCreate") == false, "runtime class creation must default to disabled")
assert(values.dig("controller", "nodeSelector") == {}, "node selector must default to empty")
assert(values.dig("podSecurityContext", "runAsNonRoot") == true, "pod security context must default to non-root")
assert(values.dig("podSecurityContext", "seccompProfile", "type") == "RuntimeDefault", "pod security context must default to RuntimeDefault seccomp")
assert(values.dig("securityContext", "allowPrivilegeEscalation") == false, "container must forbid privilege escalation by default")
assert(values.dig("securityContext", "readOnlyRootFilesystem") == true, "container root filesystem must be read-only by default")
assert(values.dig("securityContext", "capabilities", "drop") == ["ALL"], "container must drop all capabilities by default")

values_schema = YAML.load_file(File.join(CHART_DIR, "values.schema.json"))
digest_pattern = values_schema.dig("properties", "image", "properties", "digest", "pattern")
assert(digest_pattern == "^(|sha256:[a-f0-9]{64})$", "image digest schema must accept only empty or immutable sha256 references")

dockerfile = File.read(OPERATOR_DOCKERFILE)
assert(dockerfile.include?("ARG TARGETOS"), "operator build must declare BuildKit TARGETOS")
assert(dockerfile.include?("ARG TARGETARCH"), "operator build must declare BuildKit TARGETARCH")
assert(dockerfile.include?('GOOS="${TARGETOS}"'), "operator build must target BuildKit TARGETOS")
assert(dockerfile.include?('GOARCH="${TARGETARCH}"'), "operator build must target BuildKit TARGETARCH")
assert(!dockerfile.match?(/GOARCH=(?:\"?)amd64/), "operator build must not hard-code amd64")

deployment_template = File.read(File.join(CHART_DIR, "templates/deployment.yaml"))
{
  "--scheduler={{ .Values.controller.schedulerAddress }}" => "scheduler address is not configurable",
  "--control={{ .Values.controller.controlAddress }}" => "control address is not configurable",
  "--runtime={{ .Values.controller.runtimeAddress }}" => "runtime address is not configurable",
  "--listen={{ .Values.controller.listenHost }}:{{ .Values.service.port }}" => "listen address is not configurable",
  "--namespace={{ default .Release.Namespace .Values.namespaceOverride }}" => "target namespace can diverge from the release namespace",
  "--gpu-profile={{ .Values.controller.gpuProfile }}" => "gpu profile is not configurable",
  "--runtime-class-name={{ .Values.controller.runtimeClassName }}" => "runtime class name is not configurable",
  "--runtime-class-handler={{ .Values.controller.runtimeClassHandler }}" => "runtime class handler is not configurable",
  "--runtime-class-create=true" => "runtime class creation flag is not configurable",
  "--node-selector={{ $key }}={{ $value }}" => "node selector entries are not configurable",
  "--cursor-dir={{ .Values.persistence.mountPath }}" => "cursor directory is not configurable",
  "containerPort: {{ .Values.service.port }}" => "container port does not share the Service port value",
  "mountPath: {{ .Values.persistence.mountPath }}" => "cursor mount does not share the cursor-dir value",
  'default (printf "%s-state" .Values.serviceAccount.name) .Values.persistence.existingClaim' => "existingClaim does not override the managed claim"
}.each { |needle, message| assert(deployment_template.include?(needle), message) }

pvc_template = File.read(File.join(CHART_DIR, "templates/pvc.yaml"))
assert(pvc_template.include?("and .Values.persistence.enabled (not .Values.persistence.existingClaim)"), "managed PVC is not suppressed for existingClaim")
assert(pvc_template.include?("helm.sh/resource-policy: keep"), "managed PVC lacks safe uninstall retention")
assert(pvc_template.include?("storageClassName: {{ .Values.persistence.storageClass | quote }}"), "storage class is not configurable")

rbac_template = File.read(File.join(CHART_DIR, "templates/rbac.yaml"))
assert(rbac_template.include?('resources: ["jobrunbundles"]'), "Helm RBAC does not isolate jobrunbundles")
assert(rbac_template.include?('verbs: ["get", "list", "watch", "create", "update", "patch"]'), "Helm RBAC lacks jobrunbundles create")
assert(rbac_template.include?("kind: Role"), "Helm RBAC must default to a namespaced Role")
assert(rbac_template.include?("kind: RoleBinding"), "Helm RBAC must default to a namespaced RoleBinding")
assert(rbac_template.include?('{{- if .Values.controller.runtimeClassCreate }}'), "Helm RBAC must gate RuntimeClass write access on runtimeClassCreate")
assert(rbac_template.include?('verbs: ["get", "create"]'), "RuntimeClass RBAC must use only create-or-verify verbs")

chart_crd_files = Dir.glob(File.join(CHART_DIR, "crds", "**", "*.{yaml,yml}"))
assert(chart_crd_files.empty?, "operator chart must not own the externally managed CRD")
template_sources = Dir.glob(File.join(CHART_DIR, "templates", "**", "*.{yaml,yml,tpl}")).map { |path| File.read(path) }
assert(template_sources.none? { |source| source.include?("kind: CustomResourceDefinition") }, "operator chart templates must not install the external CRD")

helm, = Open3.capture2("sh", "-c", "command -v helm")
assert(!helm.strip.empty?, "helm is required for deployment contract validation")

lint_output, lint_status = Open3.capture2e("helm", "lint", CHART_DIR)
assert(lint_status.success?, "helm lint failed:\n#{lint_output}")

rendered_docs = render_chart
assert(rendered_docs.none? { |doc| doc["kind"] == "CustomResourceDefinition" }, "default Helm render must not pretend to install the external CRD")
assert_namespaced_rbac(one(rendered_docs, "Role"), one(rendered_docs, "RoleBinding"), HELM_NAMESPACE)
discovery_role, discovery_binding = cluster_rbac_for(rendered_docs, "nodes")
assert_discovery_rbac(discovery_role, discovery_binding, "tgsrl-operator", HELM_NAMESPACE)
assert(rendered_docs.count { |doc| doc["kind"] == "ClusterRole" } == 1, "default Helm render must only include discovery ClusterRole")
assert_workload_contract(rendered_docs, "tgsrl-operator-state")
assert_image_reference(rendered_docs, "tgsrl-operator:0.1.0")
rendered_pvc = one(rendered_docs, "PersistentVolumeClaim")
assert(rendered_pvc.dig("metadata", "annotations", "helm.sh/resource-policy") == "keep", "rendered PVC is not retained")

override_docs = render_chart(
  "--set",
  "image.repository=#{OVERRIDE_REPOSITORY}",
  "--set",
  "image.tag=#{OVERRIDE_TAG}",
  "--set",
  "image.digest=#{OVERRIDE_DIGEST}",
  "--set",
  "persistence.existingClaim=tgsrl-operator-precreated",
  "--set",
  "controller.runtimeClassName=kata-gpu",
  "--set",
  "controller.runtimeClassHandler=kata-qemu",
  "--set",
  "controller.runtimeClassCreate=true"
)
assert_namespaced_rbac(one(override_docs, "Role"), one(override_docs, "RoleBinding"), HELM_NAMESPACE)
override_discovery_role, override_discovery_binding = cluster_rbac_for(override_docs, "nodes")
assert_discovery_rbac(override_discovery_role, override_discovery_binding, "tgsrl-operator", HELM_NAMESPACE)
runtimeclass_role, runtimeclass_binding = cluster_rbac_for(override_docs, "runtimeclasses", "create")
assert_runtimeclass_rbac(
  runtimeclass_role,
  runtimeclass_binding,
  "tgsrl-operator",
  HELM_NAMESPACE
)
assert_workload_contract(override_docs, "tgsrl-operator-precreated")
assert_image_reference(override_docs, "#{OVERRIDE_REPOSITORY}@#{OVERRIDE_DIGEST}")

runtimeclass_args = [
  "--set", "controller.runtimeClassName=kata-gpu",
  "--set", "controller.runtimeClassHandler=kata-qemu",
  "--set", "controller.runtimeClassCreate=true"
]
base_rbac_name = runtimeclass_role.dig("metadata", "name")
other_release_docs = render_chart_for("contract-test-alt", HELM_NAMESPACE, *runtimeclass_args)
other_namespace_docs = render_chart_for(HELM_RELEASE, "tgsrl-system-alt", *runtimeclass_args)
long_identity_docs = render_chart_for("a" * 53, "b" * 63, *runtimeclass_args)
[other_release_docs, other_namespace_docs, long_identity_docs].each do |docs|
  role, binding = cluster_rbac_for(docs, "runtimeclasses", "create")
  assert_runtimeclass_rbac(role, binding, "tgsrl-operator", one(docs, "ServiceAccount").dig("metadata", "namespace"))
end
assert(cluster_rbac_for(other_release_docs, "runtimeclasses", "create").first.dig("metadata", "name") != base_rbac_name, "RuntimeClass RBAC name must include release identity")
assert(cluster_rbac_for(other_namespace_docs, "runtimeclasses", "create").first.dig("metadata", "name") != base_rbac_name, "RuntimeClass RBAC name must include release namespace identity")
ambiguous_left_docs = render_chart_for("a-b", "c", *runtimeclass_args)
ambiguous_right_docs = render_chart_for("a", "b-c", *runtimeclass_args)
assert(
  cluster_rbac_for(ambiguous_left_docs, "runtimeclasses", "create").first.dig("metadata", "name") != cluster_rbac_for(ambiguous_right_docs, "runtimeclasses", "create").first.dig("metadata", "name"),
  "RuntimeClass RBAC identity encoding must not collide across release and namespace boundaries"
)

ephemeral_docs = render_chart("--set", "persistence.enabled=false")
assert(maybe_one(ephemeral_docs, "PersistentVolumeClaim").nil?, "persistence-disabled render must not create a PVC")
assert_workload_contract(ephemeral_docs, nil)

invalid_digest_output, invalid_digest_status = run_chart(
  HELM_RELEASE,
  HELM_NAMESPACE,
  "--set-string",
  "image.digest=sha256:not-a-digest"
)
assert(!invalid_digest_status.success?, "chart schema must reject malformed image digests")
assert(
  invalid_digest_output.include?("image.digest") || invalid_digest_output.include?("/image/digest"),
  "malformed digest failure must identify image.digest"
)

puts "operator deployment contract validation passed"
