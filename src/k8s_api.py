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
    """GET `path` from `cluster`. Returns the parsed JSON body, or None if the
    request failed or returned a non-200 status (logged rather than raised —
    one unreachable cluster must not stop the others being collected)."""
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
    return resp.json()


def list_path(namespace: str, plural: str) -> str:
    """The list endpoint for an argoproj.io resource — cluster-scoped when
    `namespace` is empty, namespaced otherwise."""
    group = "/apis/argoproj.io/v1alpha1"
    return f"{group}/{plural}" if not namespace else f"{group}/namespaces/{namespace}/{plural}"


def list_items(cluster: Cluster, path: str, timeout: int, page_size: int, fetch=fetch_json):
    """Pages through a Kubernetes list endpoint and returns
    `(items, complete)`.

    `complete` is False when any page failed. That distinction matters more
    here than it looks: this exporter's consumer treats "this workflow was
    not in the response" as "it no longer exists", so silently returning the
    first page of a two-page list would read as a fleet that just lost half
    its runs. Callers skip the cluster for the cycle instead.
    """
    items = []
    params = {"limit": page_size}
    while True:
        page = fetch(cluster, path, timeout, params)
        if page is None:
            return items, False
        items.extend(page.get("items", []))

        token = (page.get("metadata") or {}).get("continue")
        if not token:
            return items, True
        params = {"limit": page_size, "continue": token}
