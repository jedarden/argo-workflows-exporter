#!/usr/bin/env bash
# Executable packaging and deployment contract for argo-workflows-exporter.
#
# The Deployment and build WorkflowTemplate are GitOps-owned by the sibling
# declarative-config checkout, so this test reads those exact files instead of
# copying a second, drift-prone manifest into this repository. It validates the
# manifest wiring and then builds the pinned image from this checkout, starts it
# with the required runtime environment, and checks the documented health
# endpoint from inside the container.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONFIG_ROOT="${DECLARATIVE_CONFIG_DIR:-$HOME/declarative-config}"
MANIFEST="${ARGO_WORKFLOWS_EXPORTER_MANIFEST:-$CONFIG_ROOT/k8s/ardenone-cluster/argo-workflows-exporter/deployment.yml}"
CONFIGMAP="${ARGO_WORKFLOWS_EXPORTER_CONFIGMAP:-$(dirname "$MANIFEST")/configmap.yml}"
BUILD_WORKFLOW="${ARGO_WORKFLOWS_EXPORTER_BUILD_WORKFLOW:-$CONFIG_ROOT/k8s/iad-ci/argo-workflows/argo-workflows-exporter-build-workflowtemplate.yml}"

for tool in docker python3; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "FAIL: required tool '$tool' is not on PATH" >&2
        exit 1
    }
done
python3 -c 'import yaml' >/dev/null 2>&1 || {
    echo "FAIL: python3 lacks pyyaml (manifest validation is a hard dependency)" >&2
    exit 1
}

for file in "$MANIFEST" "$CONFIGMAP" "$BUILD_WORKFLOW"; do
    if [ ! -f "$file" ]; then
        echo "FAIL: required GitOps file not found: $file" >&2
        echo "       set DECLARATIVE_CONFIG_DIR or the specific *_MANIFEST override" >&2
        exit 1
    fi
done

WORK="$(mktemp -d)"
CONTAINER="argo-workflows-exporter-verify-$$"
LOCAL_IMAGE=""
cleanup() {
    set +e
    if [ -n "$CONTAINER" ]; then
        docker rm -f "$CONTAINER" >/dev/null 2>&1
    fi
    if [ -n "$LOCAL_IMAGE" ]; then
        docker image rm "$LOCAL_IMAGE" >/dev/null 2>&1
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

echo "=== argo-workflows-exporter packaging contract ==="
echo "Deployment: $MANIFEST"
echo "Build workflow: $BUILD_WORKFLOW"

# Parse all YAML with duplicate-key detection. PyYAML's normal safe_load keeps
# the last duplicate key, which can make a malformed manifest appear valid.
PINNED_IMAGE_FILE="$WORK/pinned-image"
python3 - "$MANIFEST" "$CONFIGMAP" "$BUILD_WORKFLOW" "$REPO_ROOT/Dockerfile" "$PINNED_IMAGE_FILE" <<'PY'
import json
import re
import sys
from pathlib import Path

import yaml


class StrictLoader(yaml.SafeLoader):
    pass


def construct_map(loader, node):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if key in result:
            raise ValueError(f"duplicate mapping key: {key!r}")
        result[key] = loader.construct_object(value_node, deep=True)
    return result


StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    construct_map,
)


def load(path):
    with open(path, encoding="utf-8") as handle:
        return yaml.load(handle, Loader=StrictLoader)


deployment_path, configmap_path, workflow_path, dockerfile_path, image_path = sys.argv[1:]
deployment = load(deployment_path)
configmap = load(configmap_path)
workflow = load(workflow_path)
dockerfile = Path(dockerfile_path).read_text(encoding="utf-8")

assert deployment["apiVersion"] == "apps/v1"
assert deployment["kind"] == "Deployment"
assert deployment["metadata"]["name"] == "argo-workflows-exporter"

pod = deployment["spec"]["template"]["spec"]
containers = pod["containers"]
assert len(containers) == 1
container = containers[0]
assert container["name"] == "exporter"

image = container["image"]
assert re.fullmatch(r"ronaldraygun/argo-workflows-exporter:\d+\.\d+\.\d+", image), image
assert not image.endswith(":latest")

config_refs = [
    ref["configMapRef"]["name"]
    for ref in container.get("envFrom", [])
    if "configMapRef" in ref
]
assert config_refs == ["argo-workflows-exporter-config"], config_refs
assert configmap["kind"] == "ConfigMap"
assert configmap["metadata"]["name"] == "argo-workflows-exporter-config"
assert json.loads(configmap["data"]["CLUSTERS_JSON"])
assert configmap["data"]["WORKFLOW_NAMESPACE"] == "argo-workflows"

env = {entry["name"]: entry for entry in container.get("env", [])}
expected_secret_refs = {
    "DEST_S3_ENDPOINT": ("dashboard-s3-credentials", "S3_ENDPOINT"),
    "DEST_S3_ACCESS_KEY_ID": ("dashboard-s3-credentials", "ACCESS_KEY_ID"),
    "DEST_S3_SECRET_ACCESS_KEY": ("dashboard-s3-credentials", "SECRET_ACCESS_KEY"),
}
for name, (secret_name, secret_key) in expected_secret_refs.items():
    ref = env[name]["valueFrom"]["secretKeyRef"]
    assert (ref["name"], ref["key"]) == (secret_name, secret_key), (name, ref)

expected_values = {
    "DEST_S3_BUCKET": "dashboard-site",
    "DEST_S3_PREFIX": "argo/data",
    "DEST_S3_ADDRESSING_STYLE": "path",
    "POLL_INTERVAL_SECONDS": "300",
    "RUN_RETENTION_DAYS": "7",
}
for name, value in expected_values.items():
    assert env[name].get("value") == value, (name, env.get(name))

ports = container["ports"]
assert {port["name"]: port["containerPort"] for port in ports} == {"health": 8080}
for probe_name in ("livenessProbe", "readinessProbe"):
    probe = container[probe_name]
    assert probe["httpGet"] == {"path": "/health", "port": "health"}, probe

pull_secrets = [entry["name"] for entry in pod.get("imagePullSecrets", [])]
assert "docker-hub-registry" in pull_secrets, pull_secrets

assert workflow["kind"] == "WorkflowTemplate"
workflow_text = Path(workflow_path).read_text(encoding="utf-8")
assert "python -m pytest tests/ -q" in workflow_text
assert "--dockerfile=Dockerfile" in workflow_text
assert "--destination=ronaldraygun/argo-workflows-exporter:{{inputs.parameters.version}}" in workflow_text
assert not re.search(r"--destination=[^\s]+:latest(?:\s|$)", workflow_text)

assert "HEALTH_PORT=8080" in dockerfile
assert re.search(r"^EXPOSE 8080$", dockerfile, re.MULTILINE)
assert 'http://localhost:${HEALTH_PORT}/health' in dockerfile
assert 'CMD ["python", "-m", "src.main"]' in dockerfile

print(f"ok: GitOps deployment uses pinned image {image}")
print("ok: ConfigMap and dashboard-s3-credentials supply the required runtime environment")
print("ok: health port 8080 and /health liveness/readiness probes are wired")
print("ok: build WorkflowTemplate tests the checkout and publishes a semver image")
print("ok: Dockerfile exposes HEALTH_PORT and its documented health check")
with open(image_path, "w", encoding="utf-8") as handle:
    handle.write(image)
PY

PINNED_IMAGE="$(cat "$PINNED_IMAGE_FILE")"
LOCAL_IMAGE="${PINNED_IMAGE}-verify-$$"

echo "Building local verification tag from pinned image contract: $PINNED_IMAGE"
docker build --pull=false --tag "$LOCAL_IMAGE" "$REPO_ROOT"

# Keep the env file out of argv and make it private even though all values are
# deliberately non-secret loopback test values. The process under test still
# receives exactly the required variables that Kubernetes supplies.
ENV_FILE="$WORK/container.env"
umask 077
TEST_ACCESS_KEY="packaging-check-access-$$"
TEST_SECRET_KEY="packaging-check-secret-$$"
{
    printf '%s\n' 'CLUSTERS_JSON=[{"name":"packaging-check","base_url":"http://127.0.0.1:9"}]'
    printf '%s\n' 'DEST_S3_ENDPOINT=http://127.0.0.1:9'
    printf '%s\n' "DEST_S3_ACCESS_KEY_ID=$TEST_ACCESS_KEY"
    printf '%s\n' "DEST_S3_SECRET_ACCESS_KEY=$TEST_SECRET_KEY"
    printf '%s\n' 'DEST_S3_BUCKET=packaging-check-bucket'
    printf '%s\n' 'DEST_S3_PREFIX=argo/data'
    printf '%s\n' 'DEST_S3_ADDRESSING_STYLE=path'
    printf '%s\n' 'POLL_INTERVAL_SECONDS=300'
    printf '%s\n' 'RUN_RETENTION_DAYS=7'
    printf '%s\n' 'HEALTH_PORT=8080'
} >"$ENV_FILE"
unset TEST_ACCESS_KEY TEST_SECRET_KEY

echo "Starting built image and checking /health inside the container"
docker run --detach --name "$CONTAINER" --env-file "$ENV_FILE" "$LOCAL_IMAGE" >/dev/null

running=""
for _ in $(seq 1 20); do
    running="$(docker inspect --format '{{.State.Running}}' "$CONTAINER" 2>/dev/null || true)"
    [ "$running" = "true" ] && break
    sleep 1
done
if [ "$running" != "true" ]; then
    echo "FAIL: container exited before its health server started" >&2
    docker logs "$CONTAINER" >&2 || true
    exit 1
fi

status=""
for _ in $(seq 1 20); do
    status="$(docker exec "$CONTAINER" curl --silent --show-error \
        --output /tmp/health.json --write-out '%{http_code}' \
        http://127.0.0.1:8080/health 2>/dev/null || true)"
    [ "$status" = "503" ] && break
    sleep 1
done
[ "$status" = "503" ] || {
    echo "FAIL: startup health status was $status, expected 503" >&2
    docker logs "$CONTAINER" >&2 || true
    exit 1
}
docker exec "$CONTAINER" grep -F '"status":"starting"' /tmp/health.json >/dev/null
echo 'ok: built image starts and serves the documented startup health response'

python3 - "$LOCAL_IMAGE" <<'PY'
import json
import subprocess
import sys

raw = subprocess.check_output(
    ["docker", "inspect", "--format", "{{json .Config.Healthcheck}}", sys.argv[1]],
    text=True,
)
healthcheck = json.loads(raw)
assert healthcheck["Test"][0] == "CMD-SHELL"
assert "/health" in healthcheck["Test"][1]
assert "${HEALTH_PORT}" in healthcheck["Test"][1]
assert healthcheck["Interval"] == 30_000_000_000
assert healthcheck["Timeout"] == 5_000_000_000
assert healthcheck["StartPeriod"] == 10_000_000_000
assert healthcheck["Retries"] == 3
print("ok: image healthcheck uses HEALTH_PORT, /health, and documented timings")
PY

echo "PASS: container packaging and deployment wiring verified"
