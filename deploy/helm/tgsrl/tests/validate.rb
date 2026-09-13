#!/usr/bin/env ruby

require "fileutils"
require "open3"
require "tmpdir"
require "yaml"

CHART_DIR = File.expand_path("..", __dir__)
HELM_ROOT = File.expand_path("..", CHART_DIR)
REPO_ROOT = File.expand_path("../../..", CHART_DIR)
DEPLOY_SCRIPT = File.join(REPO_ROOT, "scripts", "deploy-full-stack.sh")
EXPECTED_DEPLOYMENTS = %w[
  tgsrl-console
  tgsrl-gateway
  tgsrl-job-controller
  tgsrl-operator
  tgsrl-runtime
  tgsrl-scheduler
].freeze
STATEFUL_DEPLOYMENTS = %w[tgsrl-job-controller tgsrl-operator tgsrl-runtime tgsrl-scheduler].freeze

def assert(condition, message)
  raise message unless condition
end

def documents(yaml)
  YAML.load_stream(yaml).compact
end

def resources(docs, kind)
  docs.select { |doc| doc["kind"] == kind }
end

def resource(docs, kind, name)
  matches = resources(docs, kind).select { |doc| doc.dig("metadata", "name") == name }
  assert(matches.length == 1, "expected one #{kind}/#{name}, found #{matches.length}")
  matches.first
end

def stage_chart
  root = Dir.mktmpdir("tgsrl-chart-")
  helm = File.join(root, "helm")
  FileUtils.mkdir_p(helm)
  FileUtils.cp_r(CHART_DIR, File.join(helm, "tgsrl"))
  FileUtils.cp_r(File.join(HELM_ROOT, "operator"), File.join(helm, "operator"))
  staged = File.join(helm, "tgsrl")
  output, status = Open3.capture2e("helm", "dependency", "build", staged)
  assert(status.success?, "helm dependency build failed:\n#{output}")
  [root, staged]
end

def render(chart, *args)
  output, status = Open3.capture2e(
    "helm", "template", "contract-test", chart, "--namespace", "tgsrl-system", "--include-crds", *args
  )
  assert(status.success?, "helm template failed:\n#{output}")
  documents(output)
end

root, chart = stage_chart
begin
  script_output, script_status = Open3.capture2e("bash", "-n", DEPLOY_SCRIPT)
  assert(script_status.success?, "deployment script syntax failed:\n#{script_output}")
  script_output, script_status = Open3.capture2e("bash", DEPLOY_SCRIPT, "render")
  assert(script_status.success? && script_output.include?("kind: Deployment"), "deployment script must render the staged full chart")

  script_source = File.read(DEPLOY_SCRIPT)
  %w[install upgrade].each do |action|
    help, help_status = Open3.capture2e("helm", action, "--help")
    assert(help_status.success?, "helm #{action} help failed")
    supported_flags = help.scan(/--[a-z][a-z-]*/).uniq
    command_lines = script_source.lines.select { |line| line.strip.start_with?("helm #{action} ") }
    assert(!command_lines.empty?, "deployment script must implement #{action}")
    command_lines.each do |line|
      unknown_flags = line.scan(/--[a-z][a-z-]*/).uniq - supported_flags
      assert(unknown_flags.empty?, "unsupported helm #{action} flags: #{unknown_flags.join(', ')}")
      assert(line.include?("--rollback-on-failure"), "helm #{action} must roll back failed changes")
    end
  end

  lint_output, lint_status = Open3.capture2e("helm", "lint", chart)
  assert(lint_status.success?, "helm lint failed:\n#{lint_output}")

  docs = render(chart)
  deployments = resources(docs, "Deployment")
  assert(deployments.map { |doc| doc.dig("metadata", "name") }.sort == EXPECTED_DEPLOYMENTS, "full chart must render all six services")
  assert(resources(docs, "Service").length == 6, "full chart must render one Service per control-plane component")
  assert(resources(docs, "CustomResourceDefinition").map { |doc| doc.dig("metadata", "name") } == ["jobrunbundles.tgsrl.io"], "full chart must own the external JobRunBundle prerequisite")
  chart_crd = YAML.load_file(File.join(CHART_DIR, "crds", "tgsrl_jobrunbundles.yaml"))
  canonical_crd = YAML.load_file(File.join(REPO_ROOT, "deploy", "crds", "tgsrl_jobrunbundles.yaml"))
  assert(chart_crd == canonical_crd, "umbrella chart CRD must match the canonical CRD manifest")

  deployments.each do |deployment|
    pod_spec = deployment.dig("spec", "template", "spec")
    container = pod_spec.fetch("containers").first
    assert(pod_spec["securityContext"]["runAsNonRoot"] == true, "#{deployment.dig("metadata", "name")} must run as non-root")
    assert(container.dig("securityContext", "allowPrivilegeEscalation") == false, "#{deployment.dig("metadata", "name")} must forbid privilege escalation")
    assert(container.dig("securityContext", "readOnlyRootFilesystem") == true, "#{deployment.dig("metadata", "name")} must use a read-only root filesystem")
    assert(container.dig("securityContext", "capabilities", "drop") == ["ALL"], "#{deployment.dig("metadata", "name")} must drop all Linux capabilities")
    assert(container.key?("readinessProbe") && container.key?("livenessProbe"), "#{deployment.dig("metadata", "name")} must define readiness and liveness probes")
  end

  STATEFUL_DEPLOYMENTS.each do |name|
    deployment = resource(docs, "Deployment", name)
    assert(deployment.dig("spec", "strategy", "type") == "Recreate", "#{name} must use Recreate with RWO state")
    assert(deployment.dig("spec", "template", "spec", "volumes").any? { |volume| volume["name"] == "state" || volume["name"] == "cursor-state" }, "#{name} must mount durable state")
  end

  assert(resource(docs, "Deployment", "tgsrl-runtime").dig("spec", "template", "spec", "containers", 0, "args").include?("--operator-target=tgsrl-operator:50081"), "Runtime must target the in-chart Operator")
  gateway_args = resource(docs, "Deployment", "tgsrl-gateway").dig("spec", "template", "spec", "containers", 0, "args")
  assert(gateway_args.include?("--scheduler-target=tgsrl-scheduler:50051"), "Gateway must target the in-chart Scheduler")
  assert(gateway_args.include?("--runtime-target=tgsrl-runtime:50071"), "Gateway must target the in-chart Runtime")
  console_env = resource(docs, "Deployment", "tgsrl-console").dig("spec", "template", "spec", "containers", 0, "env")
  assert(console_env.any? { |entry| entry == {"name" => "TGSRL_GATEWAY_URL", "value" => "http://tgsrl-gateway:8080"} }, "Console must proxy API traffic to the in-chart Gateway")

  network_policies = resources(docs, "NetworkPolicy").map { |doc| doc.dig("metadata", "name") }
  assert(network_policies.sort == %w[tgsrl-console-ingress tgsrl-control-plane-ingress], "default render must isolate the control plane while exposing only Console ingress")

  digest = "sha256:#{'a' * 64}"
  digest_docs = render(chart, "--set-string", "scheduler.image.digest=#{digest}")
  scheduler_image = resource(digest_docs, "Deployment", "tgsrl-scheduler").dig("spec", "template", "spec", "containers", 0, "image")
  assert(scheduler_image == "tgsrl-scheduler@#{digest}", "immutable digest must override the Scheduler tag")

  config_docs = render(
    chart,
    "--set", "config.existingConfigMap=tgsrl-config",
    "--set", "config.items[0].key=manifest",
    "--set", "config.items[0].path=compatibility/manifests/cpu-mock.yaml"
  )
  %w[tgsrl-scheduler tgsrl-runtime].each do |name|
    deployment = resource(config_docs, "Deployment", name)
    pod_spec = deployment.dig("spec", "template", "spec")
    assert(pod_spec.fetch("volumes").any? { |volume| volume.dig("configMap", "name") == "tgsrl-config" }, "#{name} must mount the selected configuration graph")
    assert(pod_spec.fetch("containers").first.fetch("args").include?("--config-root=/etc/tgsrl/config"), "#{name} must read the mounted configuration graph")
  end

  registry_docs = render(
    chart,
    "--set", "scheduler.workerRegistry.enabled=true",
    "--set", "scheduler.workerRegistry.signingKeySecret=tgsrl-worker-registry",
    "--set", "operator.controller.workerBootstrap.enabled=true",
    "--set", "operator.controller.workerBootstrap.installerImage=registry.example.test/tgsrl/bootstrap@sha256:#{'b' * 64}",
    "--set", "operator.controller.workerBootstrap.registryURL=http://tgsrl-scheduler:50091",
    "--set", "operator.controller.workerBootstrap.registrySigningKeySecret=tgsrl-worker-registry"
  )
  scheduler = resource(registry_docs, "Deployment", "tgsrl-scheduler")
  scheduler_args = scheduler.dig("spec", "template", "spec", "containers", 0, "args")
  assert(scheduler_args.include?("--worker-registry-runtime-target=tgsrl-runtime:50071"), "worker registry must publish lifecycle to Runtime")
  assert(scheduler.dig("spec", "template", "spec", "volumes").any? { |volume| volume.dig("secret", "secretName") == "tgsrl-worker-registry" }, "Scheduler must mount the shared signing key")
  runtime = resource(registry_docs, "Deployment", "tgsrl-runtime")
  runtime_args = runtime.dig("spec", "template", "spec", "containers", 0, "args")
  assert(runtime_args.include?("--worker-registry-signing-key-file=/var/run/secrets/tgsrl-worker-registry/signing-key"), "Runtime must authenticate worker trace requests with the shared signing key")
  assert(runtime.dig("spec", "template", "spec", "volumes").any? { |volume| volume.dig("secret", "secretName") == "tgsrl-worker-registry" }, "Runtime must mount the shared signing key")

  gpu_docs = render(
    chart,
    "--set", "scheduler.nvidia.enabled=true",
    "--set-string", "scheduler.nvidia.image.digest=sha256:#{'c' * 64}",
    "--set", "scheduler.nvidia.partitionMode=auto",
    "--set", "scheduler.nvidia.runtimeClassName=nvidia",
    "--set-json", 'scheduler.nvidia.nodeSelector={"kubernetes.io/hostname":"gpu-node-a"}',
    "--set", "scheduler.workerRegistry.enabled=true",
    "--set", "scheduler.workerRegistry.signingKeySecret=tgsrl-worker-registry",
    "--set", "scheduler.manifest=compatibility/manifests/gpu-smoke-verl.yaml",
    "--set", "operator.controller.gpuProfile=kubernetes-dra",
    "--set-json", 'operator.controller.nodeSelector={"kubernetes.io/hostname":"gpu-node-a"}',
    "--set", "operator.controller.workerBootstrap.enabled=true",
    "--set", "operator.controller.workerBootstrap.installerImage=registry.example.test/tgsrl/bootstrap@sha256:#{'b' * 64}",
    "--set", "operator.controller.workerBootstrap.registryURL=http://tgsrl-scheduler:50091",
    "--set", "operator.controller.workerBootstrap.registrySigningKeySecret=tgsrl-worker-registry",
    "--set", "operator.controller.workerBootstrap.verifyDeviceIdentities=true"
  )
  gpu_scheduler = resource(gpu_docs, "Deployment", "tgsrl-scheduler")
  gpu_pod = gpu_scheduler.dig("spec", "template", "spec")
  gpu_args = gpu_pod.dig("containers", 0, "args")
  assert(gpu_pod["runtimeClassName"] == "nvidia", "NVIDIA Scheduler must use the selected runtime class")
  assert(gpu_pod.dig("containers", 0, "image") == "tgsrl-scheduler-nvidia@sha256:#{'c' * 64}", "NVIDIA Scheduler must use the dedicated immutable image")
  assert(gpu_pod["nodeSelector"] == {"kubernetes.io/hostname" => "gpu-node-a"}, "NVIDIA Scheduler must be pinned to the inventory node")
  assert(gpu_args.include?("--nvidia-driver-v2=true"), "NVIDIA Scheduler must enable Driver v2")
  assert(gpu_args.include?("--nvidia-partition-mode=auto"), "NVIDIA partition mode is not projected")
  assert(gpu_args.include?("--worker-registry-state=/var/lib/tgsrl-scheduler/nvidia-runtime.json"), "NVIDIA runtime helper and registry must share worker state")
  gpu_env = gpu_pod.dig("containers", 0, "env")
  assert(gpu_env.include?({"name" => "NVIDIA_VISIBLE_DEVICES", "value" => "all"}), "NVIDIA Scheduler device visibility is not configured")
  gpu_operator_args = resource(gpu_docs, "Deployment", "tgsrl-operator").dig("spec", "template", "spec", "containers", 0, "args")
  assert(gpu_operator_args.include?("--node-selector=kubernetes.io/hostname=gpu-node-a"), "managed workloads must be pinned to the Scheduler inventory node")

  failure, status = Open3.capture2e("helm", "template", "contract-test", chart, "--set", "scheduler.workerRegistry.enabled=true")
  assert(!status.success? && failure.include?("signingKeySecret"), "registry render must fail when its signing-key Secret is missing")

  failure, status = Open3.capture2e(
    "helm", "template", "contract-test", chart,
    "--set", "operator.controller.workerBootstrap.enabled=true",
    "--set", "operator.controller.workerBootstrap.installerImage=registry.example.test/tgsrl/bootstrap@sha256:#{'b' * 64}",
    "--set", "operator.controller.workerBootstrap.registryURL=http://tgsrl-scheduler:50091",
    "--set", "operator.controller.workerBootstrap.registrySigningKeySecret=tgsrl-worker-registry"
  )
  assert(!status.success? && failure.include?("scheduler.workerRegistry.enabled"), "bootstrap render must require the in-chart worker registry")

  {
    "scheduler.nvidia.enabled=true" => "workerRegistry.enabled",
    "scheduler.nvidia.enabled=true,--set,scheduler.workerRegistry.enabled=true,--set,scheduler.workerRegistry.signingKeySecret=tgsrl-worker-registry" => "workerBootstrap.enabled"
  }.each do |settings, message|
    args = settings.split(",--set,").flat_map { |setting| ["--set", setting] }
    failure, status = Open3.capture2e("helm", "template", "contract-test", chart, *args)
    assert(!status.success? && failure.include?(message), "incomplete NVIDIA Helm mode must fail on #{message}")
  end

  failure, status = Open3.capture2e(
    "helm", "template", "contract-test", chart, "--namespace", "tgsrl-system",
    "--set", "operator.namespaceOverride=other-namespace"
  )
  assert(!status.success? && failure.include?("namespaceOverride"), "umbrella chart must reject an Operator namespace split")
ensure
  FileUtils.remove_entry(root) if root && File.exist?(root)
end

puts "full-stack deployment contract validation passed"
