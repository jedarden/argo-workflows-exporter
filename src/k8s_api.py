"""Uniform REST access to a cluster's Kubernetes API — either the local
in-cluster API server (authenticated with the pod's own ServiceAccount) or a
remote cluster's read-only, credential-free proxy reached over whatever
network path makes its base URL resolvable from the pod. Both paths return
the same raw API JSON, so callers never need to know which kind of cluster
they are talking to.
"""

import logging

import requests

from .config import Cluster

log = logging.getLogger(__name__)

_SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_SA_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
_LOCAL_API_SERVER = "https://kubernetes.default.svc"


class KubernetesResponseError(ValueError):
    """A successful Kubernetes request returned an unusable JSON body.

    Request failures are represented by ``list_items`` returning
    ``([], False)`` because they are expected to be transient. A response
    that parses as JSON but violates the list contract is different: treating
    it as an empty list would make every object on that cluster look deleted.
    Callers can catch this error per cluster and keep it out of the snapshot.
    """


def _local_request(path: str, params: dict, timeout: int) -> requests.Response:
    with open(_SA_TOKEN_PATH) as f:
        token = f.read().strip()
    return requests.get(
        f"{_LOCAL_API_SERVER}{path}",
        params=params,
        headers={"Authorization": f"Bearer {token}"},
        verify=_SA_CA_PATH,
        timeout=timeout,
    )


def fetch_json(cluster: Cluster, path: str, timeout: int, params: dict | None = None):
    """GET `path` from `cluster` and return its parsed JSON body.

    Request failures and non-200 responses return ``None`` so one unreachable
    cluster does not stop the others being collected. Invalid JSON is raised
    as ``KubernetesResponseError``: it is a malformed answer, not an empty
    answer, and must not silently erase a cluster's snapshot.
    """
    try:
        if cluster.base_url is None:
            resp = _local_request(path, params or {}, timeout)
        else:
            resp = requests.get(f"{cluster.base_url}{path}", params=params or {}, timeout=timeout)
    except requests.RequestException as e:
        log.warning("%s: request failed for %s: %s", cluster.name, path, e)
        return None

    if resp.status_code != 200:
        log.warning("%s: %s -> HTTP %d", cluster.name, path, resp.status_code)
        return None
    try:
        body = resp.json()
    except ValueError as e:
        raise KubernetesResponseError(
            f"{cluster.name}: invalid JSON response from {path}: {e}"
        ) from e
    if body is None:
        # ``None`` is the transport-failure sentinel used by list_items, so a
        # successful JSON null must not be allowed to masquerade as a failed
        # request and quietly become an empty result.
        raise KubernetesResponseError(
            f"{cluster.name}: malformed JSON response from {path}: expected an object"
        )
    return body


def list_path(namespace: str, plural: str) -> str:
    """The list endpoint for an argoproj.io resource — cluster-scoped when
    `namespace` is empty, namespaced otherwise."""
    group = "/apis/argoproj.io/v1alpha1"
    return f"{group}/{plural}" if not namespace else f"{group}/namespaces/{namespace}/{plural}"


def list_items(cluster: Cluster, path: str, timeout: int, page_size: int, fetch=fetch_json):
    """Pages through a Kubernetes list endpoint and returns
    `(items, complete)`.

    `complete` is False when any page failed. In that case the returned items
    are always discarded. That distinction matters more here than it looks:
    this exporter's consumer treats "this workflow was not in the response"
    as "it no longer exists", so returning the first page of a failed
    two-page list would read as a fleet that just lost half its runs. Callers
    skip the cluster for the cycle instead.

    A successful response whose JSON shape violates the Kubernetes list
    contract raises ``KubernetesResponseError``. It is never converted into
    an empty or partial result.
    """
    items = []
    params = {"limit": page_size}
    seen_tokens = set()
    while True:
        page = fetch(cluster, path, timeout, params)
        if page is None:
            return [], False
        if not isinstance(page, dict):
            raise KubernetesResponseError(
                f"{cluster.name}: malformed list response from {path}: "
                f"expected a JSON object, got {type(page).__name__}"
            )
        if "items" not in page:
            raise KubernetesResponseError(
                f"{cluster.name}: malformed list response from {path}: "
                "missing 'items'"
            )
        page_items = page["items"]
        if not isinstance(page_items, list):
            raise KubernetesResponseError(
                f"{cluster.name}: malformed list response from {path}: "
                f"'items' must be a JSON array, got {type(page_items).__name__}"
            )
        items.extend(page_items)

        metadata = page.get("metadata")
        if metadata is None:
            token = None
        elif not isinstance(metadata, dict):
            raise KubernetesResponseError(
                f"{cluster.name}: malformed list response from {path}: "
                f"'metadata' must be a JSON object, got {type(metadata).__name__}"
            )
        elif "continue" not in metadata:
            token = None
        else:
            token = metadata["continue"]
            if token is None or not isinstance(token, str):
                raise KubernetesResponseError(
                    f"{cluster.name}: malformed list response from {path}: "
                    "'metadata.continue' must be a string"
                )
            if token and not token.strip():
                raise KubernetesResponseError(
                    f"{cluster.name}: malformed list response from {path}: "
                    "'metadata.continue' must not be whitespace"
                )
        if not token:
            return items, True
        if token in seen_tokens:
            raise KubernetesResponseError(
                f"{cluster.name}: malformed list response from {path}: "
                f"continuation token repeated: {token!r}"
            )
        seen_tokens.add(token)
        params = {"limit": page_size, "continue": token}
