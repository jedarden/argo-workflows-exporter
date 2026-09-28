# Production deployment and upgrade runbook

This runbook describes the production installation on `ardenone-cluster` and
the release process for the exporter. The application repository contains the
image and runtime contract. The sibling `declarative-config` repository is the
source of truth for the Kubernetes resources and must be changed for every
production deployment.

## Production inventory

The production resources are under
`k8s/ardenone-cluster/argo-workflows-exporter/` in `declarative-config`:

- `namespace.yml` creates the `argo-workflows-exporter` namespace.
- `configmap.yml` supplies `CLUSTERS_JSON` and `WORKFLOW_NAMESPACE`.
- `deployment.yml` supplies the image, S3 references, literal settings, one
  replica, and the health probes.
- The namespace is reconciled by the cluster's namespace ApplicationSet; do
  not add a second hand-written Application for this plain manifest directory.

The current production shape polls `iad-ci` through the private
`kubectl-proxy-iad-ci-egress` service and writes these objects to the
`dashboard-site` bucket under `argo/data/`:

```text
workflows.parquet
runs.parquet
meta.json
```

The image tag in `deployment.yml`, not the `VERSION` file in this checkout, is
the authoritative statement of what production is running. Keep the
deployment at one replica with `strategy.type: Recreate`: `runs.parquet` is a
read-modify-write ledger, and overlapping writers can lose observations or
interleave generations.

## Release and image versioning

Images are semver-tagged as
`ronaldraygun/argo-workflows-exporter:<major>.<minor>.<patch>`. Use a new
immutable semver tag for every release. Never deploy a floating tag or a bare
commit identifier.

The build workflow is
`k8s/iad-ci/argo-workflows/argo-workflows-exporter-build-workflowtemplate.yml`
in `declarative-config`. A push to `main` starts the configured Argo Events
sensor, which submits that workflow. The workflow:

1. clones the Forgejo `main` branch and runs `python -m pytest tests/ -q`;
2. resolves the release version after tests pass. If the pushed commit
   changed `VERSION`, that value is used. Otherwise the workflow increments
   the patch component, commits the new `VERSION` with the CI identity, and
   pushes that commit back to `main`;
3. builds with Kaniko using the resolved version as both the image tag and the
   `VERSION` build argument.

The auto-version commit is excluded from the build sensor, so it does not
create an endless build loop. Wait for the workflow to pass and confirm the
semver tag exists before changing the production Deployment. A source commit
being merged does not deploy it automatically; the image tag still has to be
advanced in `declarative-config`.

Before a production release, run the packaging contract from a checkout that
also has `declarative-config`:

```bash
tests/container-packaging/run.sh
```

This validates the Deployment and ConfigMap wiring, the S3 secret references,
the probes, the build workflow, and the image's startup health check. It also
builds the image locally and verifies its initial `/health` response. Set
`DECLARATIVE_CONFIG_DIR` if the sibling checkout is not at
`$HOME/declarative-config`.

## Runtime configuration

Configuration is loaded once at startup. Missing required values or invalid
numeric values terminate the process before the health server starts.

### Required and optional environment variables

| Variable | Required | Default | Production meaning |
| --- | --- | --- | --- |
| `CLUSTERS_JSON` | yes | — | Non-empty JSON array of unique cluster names. Each entry may contain `base_url` and a namespace override. |
| `DEST_S3_ENDPOINT` | yes | — | S3-compatible endpoint. |
| `DEST_S3_ACCESS_KEY_ID` | yes | — | S3 access key supplied by a Kubernetes Secret reference. |
| `DEST_S3_SECRET_ACCESS_KEY` | yes | — | S3 secret key supplied by a Kubernetes Secret reference. |
| `DEST_S3_BUCKET` | yes | — | Destination bucket. |
| `WORKFLOW_NAMESPACE` | no | empty (all namespaces) | Namespace used for entries without a per-cluster `namespace`. Set this to keep the Kubernetes list request namespaced. |
| `DEST_S3_PREFIX` | no | `argo/data` | Key prefix for all three output objects. An explicitly empty value writes at the bucket root. |
| `DEST_S3_ADDRESSING_STYLE` | no | `virtual` | `auto`, `virtual`, or `path`; production Garage uses `path`. |
| `DEST_S3_REGION` | no | `us-east-1` | Region passed to the S3 client. |
| `POLL_INTERVAL_SECONDS` | no | `300` | Delay after each cycle. Keep the full cadence comfortably below the shortest Argo workflow TTL. |
| `RUN_RETENTION_DAYS` | no | `7` | Days a run remains after its last observation. Must be at least `1`. |
| `HTTP_TIMEOUT_SECONDS` | no | `10` | Timeout for each Kubernetes API request. |
| `LIST_PAGE_SIZE` | no | `500` | Kubernetes list page size. Continuation tokens are followed to completion. |
| `HEALTH_PORT` | no | `8080` | Port for `GET /health`. |
| `LOG_LEVEL` | no | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` (aliases `WARN` and `FATAL` are accepted). |
| `VERSION_FILE` | no | `VERSION` | File read at startup and reported in `meta.json`; the image copies the repository `VERSION` file to `/app/VERSION`. |

In production, `CLUSTERS_JSON` contains the `iad-ci` proxy URL and
`WORKFLOW_NAMESPACE` is `argo-workflows`. The Deployment gets those values
through `envFrom` from `argo-workflows-exporter-config`. The other settings
are explicit `env` entries in `deployment.yml`:

```text
DEST_S3_BUCKET=dashboard-site
DEST_S3_PREFIX=argo/data
DEST_S3_ADDRESSING_STYLE=path
POLL_INTERVAL_SECONDS=300
RUN_RETENTION_DAYS=7
```

Do not put access keys, secret keys, or ServiceAccount tokens in
`CLUSTERS_JSON`, a ConfigMap, a command line, or a committed manifest.

### S3 credential references

The three credential-bearing environment variables use `secretKeyRef` and
must remain references, not literal values:

| Environment variable | Secret | Secret key |
| --- | --- | --- |
| `DEST_S3_ENDPOINT` | `dashboard-s3-credentials` | `S3_ENDPOINT` |
| `DEST_S3_ACCESS_KEY_ID` | `dashboard-s3-credentials` | `ACCESS_KEY_ID` |
| `DEST_S3_SECRET_ACCESS_KEY` | `dashboard-s3-credentials` | `SECRET_ACCESS_KEY` |

`dashboard-s3-credentials` is generated from the `dashboard-write-key`
`GarageKey` in `k8s/ardenone-cluster/garage-operator/keys.yml` and reflected
into the application namespace. Its bucket permission is `dashboard-site`
with read and write access. Rotate or repair that credential at its owning
Garage/OpenBao/secret-management source; do not create a populated Secret in
the application repository. After a rotation, read-only inspection should
confirm that the reflected Secret exists and that the pod becomes healthy
again.

## Cluster access, proxy mode, and RBAC

Each `CLUSTERS_JSON` entry chooses exactly one transport:

- With `base_url` omitted, the entry is local. The exporter calls
  `https://kubernetes.default.svc` with the pod's projected ServiceAccount
  token and CA at
  `/var/run/secrets/kubernetes.io/serviceaccount/token` and
  `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`. At most one local
  entry is allowed. The files are opened for every request, so projected-token
  rotation is picked up without restarting the pod.
- With `base_url` present, the entry is proxied. The exporter sends an
  unauthenticated HTTP GET to that URL and does not send a Kubernetes token.
  The URL must resolve from the pod over the private network path. This is the
  production mode for `iad-ci`.

The exporter only lists Argo Workflows. The minimum Kubernetes permission is:

```yaml
- apiGroups: ["argoproj.io"]
  resources: ["workflows"]
  verbs: ["get", "list"]
```

`watch` is not needed: the exporter uses paginated GET/list requests and a
poll interval. For a local entry, bind a dedicated ServiceAccount to a Role
in the watched namespace when `WORKFLOW_NAMESPACE` or the per-cluster
namespace is set. If the list is cluster-scoped, use a ClusterRole and
ClusterRoleBinding instead. The ServiceAccount must be in the exporter pod's
namespace and its token automount must remain enabled.

For a proxied entry, grant the same read-only permission to the identity used
by the proxy in the target cluster. The production `iad-ci` proxy's existing
`devpod-observer` ClusterRole includes `get`, `list`, and `watch` for
`argoproj.io/workflows`; the exporter itself has no direct RBAC grant in
`ardenone-cluster` because it never uses the local API server there. Do not
add S3 permissions to Kubernetes RBAC and do not broaden the exporter to
mutate workflows.

## Health probes and operational checks

The image exposes port `8080` and its container health check calls
`GET /health`. The production Deployment wires both liveness and readiness to
the named `health` port and `/health`, with a 15-second initial delay. The
endpoint is intentionally a publication-health signal:

- `503` with `status: starting` means no complete cycle has published yet.
- `200` with `status: ok` means all three objects were published by the last
  successful cycle.
- `503` with `status: stale` means the last success is at least two polling
  intervals old. With the production 300-second interval, that threshold is
  600 seconds.

`meta.json` is uploaded last and is the publication commit marker. A failed
read or compute phase writes nothing. A failed upload may leave a torn set,
but all three objects carry a generation identity so consumers can reject
the mixed set and retain the last complete generation. A subsequent cycle
retries the publication.

For a rollout or incident, inspect the following read-only signals in order:

1. ArgoCD Application sync and health for the namespace application.
2. Deployment, Pod, and Event status in `argo-workflows-exporter`.
3. Current pod logs for configuration errors, proxy failures, S3 failures,
   and the `failure_phase`/`publication` context on failed cycles.
4. `GET /health`; then confirm a fresh `meta.json` reports the expected
   version, `iad-ci` as reachable, and matching generation IDs in the sidecar
   and both Parquet footers.

## GitOps deployment procedure

1. Confirm the image build workflow completed successfully and record the
   immutable semver tag.
2. In the `declarative-config` checkout, inspect the worktree and update only
   `k8s/ardenone-cluster/argo-workflows-exporter/deployment.yml` so
   `spec.template.spec.containers[0].image` names that tag. Keep the
   `Recreate` strategy, one replica, pull secret, environment references, and
   probes unchanged unless the release explicitly requires a reviewed config
   change.
3. Run the application packaging contract from the exporter checkout and
   validate the declarative-config diff. Commit the manifest change to
   `main` with a message that includes the deployed version, then push to the
   configured `origin`.
4. Let the namespace ApplicationSet and ArgoCD reconcile the pushed desired
   state. The `Recreate` strategy terminates the old writer before starting
   the new writer; do not manually mutate the live Deployment or restart the
   pod.
5. Verify ArgoCD reports `Synced` and `Healthy`, then perform the health and
   S3 checks above. The first successful cycle should update `meta.json` with
   the new exporter version.

The application repository's `main` commit and the declarative-config image
update are separate changes. Record both commit IDs in the deployment change
or operating notes so the running image can be traced back to source.

## Rollback procedure

Rollback is another GitOps image change, not a live Kubernetes command:

1. Identify the last known-good image tag from the declarative-config history
   and from the last healthy `meta.json`.
2. Make a new commit that changes only the Deployment image back to that tag.
   Avoid reverting an entire unrelated manifest commit.
3. Push the commit to `origin/main` and wait for ArgoCD to reconcile the
   namespace Application. Confirm the old pod is gone before treating the
   replacement as the active writer.
4. Inspect logs and `/health` until the rollback publishes a complete cycle.
   Confirm the sidecar and Parquet footer generation IDs agree. Do not delete
   `workflows.parquet`, `runs.parquet`, or `meta.json` to force recovery: a
   failed new pod normally leaves the previous sidecar generation available.

Before rolling back across a data-contract boundary, use the compatibility
rules below. If the old binary cannot read the current stored objects, deploy
the compatible consumer/producer pair or use a new S3 prefix for the test;
do not experiment against `argo/data`.

## Upgrades across schema and generation-identity changes

Treat an exporter upgrade as both an image rollout and a producer-contract
change. The Deployment's single-writer `Recreate` strategy prevents concurrent
cycles, but it does not migrate arbitrary incompatible data types.

### Additive or reordered Parquet columns

The exporter reads the existing ledger and conforms it to the current
`RUNS_SCHEMA` by column name. A column introduced by a newer release is null
for old rows until those runs are observed again; an old extra column is
dropped when the next publication is written. Existing rows, identities, and
retention timestamps are preserved. This is the normal path for additive
fields such as the failure taxonomy.

For this class of upgrade:

1. Confirm consumers normalize each stored object to the current schema and
   do not concatenate raw Parquet schemas from different generations.
2. Roll out the new image through GitOps.
3. Wait for one successful cycle and verify that the new `meta.json` version,
   row counts, and schema are visible. No manual rewrite or object deletion is
   required.

Changing a field's type, meaning, or identity key is not an additive change.
Stop and define a versioned migration or a new prefix before deploying it.

### Generation identity changes

Current publications mint a fresh
`<generated_at>-<12 lowercase hex characters>` `generation_id` per cycle. The
same non-empty ID is stored in `meta.json` and in the file metadata of both
Parquet objects. Consumers must expose a generation only when all three IDs
match; `meta.json` remains the commit marker and is uploaded last.

A `runs.parquet` written before generation identity existed is valid ledger
input but has no publication identity. The first successful cycle after an
upgrade reuses its rows and writes a fresh matching ID to all three current
objects. A failed read or compute leaves the old publication untouched; a
failed upload is retried and must not be treated as a complete new generation.

Do not roll back to a producer that predates generation identity while
leaving a generation-aware consumer pointed at the same prefix without a
compatibility decision. Such a producer can overwrite the Parquet footers and
sidecar contract with legacy objects. Either keep the generation-aware
producer, coordinate the consumer rollback as one change, or validate the old
producer on an isolated prefix.

After any schema or generation change, record the producer version, the first
successful generation ID, and the consumer verification result with the two
Git commit IDs. This makes a later rollback auditable without exposing S3
credentials.
