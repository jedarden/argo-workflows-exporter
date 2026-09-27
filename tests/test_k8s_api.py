from types import SimpleNamespace

import pytest

from src.config import Cluster
from src import k8s_api
from src.k8s_api import KubernetesResponseError, fetch_json, list_items, list_path

_CLUSTER = Cluster(name="ci", base_url="http://proxy.example:8001")


def _pages(*pages):
    """A fetch_json stand-in that returns each page in turn; None means the
    request failed."""
    calls = []

    def fetch(cluster, path, timeout, params=None):
        calls.append(params or {})
        return pages[len(calls) - 1]

    return fetch, calls


def test_list_path_is_cluster_scoped_when_no_namespace_is_set():
    assert list_path("", "workflows") == "/apis/argoproj.io/v1alpha1/workflows"


def test_list_path_is_namespaced_when_a_namespace_is_set():
    assert (
        list_path("argo", "workflows")
        == "/apis/argoproj.io/v1alpha1/namespaces/argo/workflows"
    )


def test_single_page_listing():
    fetch, _ = _pages({"items": [{"a": 1}], "metadata": {}})
    items, complete = list_items(_CLUSTER, "/p", 10, 500, fetch=fetch)
    assert (items, complete) == ([{"a": 1}], True)


def test_paging_follows_each_continue_token_and_completes():
    fetch, calls = _pages(
        {"items": [{"a": 1}], "metadata": {"continue": "tok-1"}},
        {"items": [{"a": 2}], "metadata": {"continue": "tok-2"}},
        {"items": [{"a": 3}], "metadata": {}},
    )
    items, complete = list_items(_CLUSTER, "/p", 10, 500, fetch=fetch)

    assert complete is True
    assert items == [{"a": 1}, {"a": 2}, {"a": 3}]
    assert calls == [
        {"limit": 500},
        {"limit": 500, "continue": "tok-1"},
        {"limit": 500, "continue": "tok-2"},
    ]


def test_a_failed_page_marks_the_listing_incomplete():
    """A truncated list would read as workflows having been deleted."""
    fetch, _ = _pages({"items": [{"a": 1}], "metadata": {"continue": "tok"}}, None)
    items, complete = list_items(_CLUSTER, "/p", 10, 500, fetch=fetch)
    assert complete is False
    assert items == []


def test_first_page_failure_is_incomplete_and_empty():
    fetch, _ = _pages(None)
    assert list_items(_CLUSTER, "/p", 10, 500, fetch=fetch) == ([], False)


def test_invalid_json_is_surfaced_as_a_malformed_response(monkeypatch):
    def fake_get(*args, **kwargs):
        response = SimpleNamespace(status_code=200)
        response.json = lambda: (_ for _ in ()).throw(ValueError("bad JSON"))
        return response

    monkeypatch.setattr(k8s_api.requests, "get", fake_get)
    with pytest.raises(KubernetesResponseError, match="invalid JSON"):
        fetch_json(_CLUSTER, "/p", 17)


def test_successful_json_null_is_not_treated_as_a_transport_failure(monkeypatch):
    def fake_get(*args, **kwargs):
        return SimpleNamespace(status_code=200, json=lambda: None)

    monkeypatch.setattr(k8s_api.requests, "get", fake_get)
    with pytest.raises(KubernetesResponseError, match="malformed JSON"):
        fetch_json(_CLUSTER, "/p", 17)


@pytest.mark.parametrize(
    "page",
    [
        pytest.param({}, id="missing-items"),
        pytest.param({"items": {}}, id="items-object"),
        pytest.param({"items": None}, id="items-null"),
        pytest.param({"items": [], "metadata": []}, id="metadata-array"),
    ],
)
def test_malformed_list_shapes_are_surfaced(page):
    fetch, _ = _pages(page)
    with pytest.raises(KubernetesResponseError, match="malformed list response"):
        list_items(_CLUSTER, "/p", 10, 500, fetch=fetch)


@pytest.mark.parametrize(
    "token",
    [
        pytest.param(None, id="null"),
        pytest.param(123, id="number"),
        pytest.param([], id="array"),
        pytest.param({}, id="object"),
        pytest.param("   ", id="whitespace"),
    ],
)
def test_malformed_continuation_tokens_are_surfaced(token):
    fetch, _ = _pages({"items": [], "metadata": {"continue": token}})
    with pytest.raises(KubernetesResponseError, match="metadata.continue"):
        list_items(_CLUSTER, "/p", 10, 500, fetch=fetch)


def test_repeated_continuation_token_is_surfaced_instead_of_looping():
    fetch, calls = _pages(
        {"items": [{"a": 1}], "metadata": {"continue": "same"}},
        {"items": [{"a": 2}], "metadata": {"continue": "same"}},
    )
    with pytest.raises(KubernetesResponseError, match="repeated"):
        list_items(_CLUSTER, "/p", 10, 500, fetch=fetch)
    assert calls == [{"limit": 500}, {"limit": 500, "continue": "same"}]


def test_malformed_later_page_cannot_return_the_first_page_as_a_partial_result():
    fetch, _ = _pages(
        {"items": [{"a": 1}], "metadata": {"continue": "next"}},
        {"items": {}, "metadata": {}},
    )
    with pytest.raises(KubernetesResponseError):
        list_items(_CLUSTER, "/p", 10, 500, fetch=fetch)


def test_fetch_json_issues_a_plain_get_with_no_watch_parameter(monkeypatch):
    """The RBAC the README asks for is get/list only. Pin the HTTP behavior
    that keeps that true: a single non-streaming GET carrying no `watch`
    request parameter — the shape a watch-based client would not have."""
    calls = {}

    def fake_get(url, params=None, **kwargs):
        calls["url"] = url
        calls["params"] = params
        calls["kwargs"] = kwargs
        resp = SimpleNamespace(status_code=200)
        resp.json = lambda: {"items": [{"a": 1}], "metadata": {}}
        return resp

    monkeypatch.setattr(k8s_api.requests, "get", fake_get)
    body = fetch_json(
        _CLUSTER, "/apis/argoproj.io/v1alpha1/namespaces/argo/workflows", 10
    )

    assert body == {"items": [{"a": 1}], "metadata": {}}
    assert (
        calls["url"]
        == "http://proxy.example:8001/apis/argoproj.io/v1alpha1/namespaces/argo/workflows"
    )
    assert calls["params"] == {}
    assert calls["kwargs"].get("stream") is not True


def test_fetch_json_routes_a_proxied_cluster_and_passes_the_timeout(monkeypatch):
    calls = {}

    def fake_get(url, params=None, **kwargs):
        calls.update(url=url, params=params, kwargs=kwargs)
        return SimpleNamespace(
            status_code=200,
            json=lambda: {"items": [{"a": 1}], "metadata": {}},
        )

    monkeypatch.setattr(k8s_api.requests, "get", fake_get)
    body = fetch_json(
        _CLUSTER,
        "/apis/argoproj.io/v1alpha1/workflows",
        17,
        params={"limit": 2},
    )

    assert body == {"items": [{"a": 1}], "metadata": {}}
    assert calls == {
        "url": "http://proxy.example:8001/apis/argoproj.io/v1alpha1/workflows",
        "params": {"limit": 2},
        "kwargs": {"timeout": 17},
    }


def test_fetch_json_routes_a_local_cluster_through_the_service_account(monkeypatch):
    calls = {}

    def fake_local_request(path, params, timeout):
        calls.update(path=path, params=params, timeout=timeout)
        return SimpleNamespace(
            status_code=200,
            json=lambda: {"items": [{"a": 1}], "metadata": {}},
        )

    monkeypatch.setattr(k8s_api, "_local_request", fake_local_request)
    body = fetch_json(
        Cluster(name="local"),
        "/apis/argoproj.io/v1alpha1/workflows",
        10,
        params={"limit": 500},
    )

    assert body == {"items": [{"a": 1}], "metadata": {}}
    assert calls == {
        "path": "/apis/argoproj.io/v1alpha1/workflows",
        "params": {"limit": 500},
        "timeout": 10,
    }


def test_local_request_uses_the_service_account_and_passes_the_timeout(
    tmp_path, monkeypatch
):
    token_path = tmp_path / "token"
    token_path.write_text("sa-token\n")
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text("certificate")
    monkeypatch.setattr(k8s_api, "_SA_TOKEN_PATH", str(token_path))
    monkeypatch.setattr(k8s_api, "_SA_CA_PATH", str(ca_path))

    calls = {}

    def fake_get(url, **kwargs):
        calls.update(url=url, kwargs=kwargs)
        return SimpleNamespace(status_code=200)

    monkeypatch.setattr(k8s_api.requests, "get", fake_get)
    response = k8s_api._local_request(
        "/apis/argoproj.io/v1alpha1/namespaces/argo/workflows",
        {"limit": 2},
        17,
    )

    assert response.status_code == 200
    assert calls == {
        "url": "https://kubernetes.default.svc/apis/argoproj.io/v1alpha1/namespaces/argo/workflows",
        "kwargs": {
            "params": {"limit": 2},
            "headers": {"Authorization": "Bearer sa-token"},
            "verify": str(ca_path),
            "timeout": 17,
        },
    }


@pytest.mark.parametrize(
    ("cluster", "expected_url", "expected_headers"),
    [
        pytest.param(
            Cluster(name="local"),
            "https://kubernetes.default.svc/apis/argoproj.io/v1alpha1/workflows",
            {"Authorization": "Bearer sa-token"},
            id="local-service-account",
        ),
        pytest.param(
            _CLUSTER,
            "http://proxy.example:8001/apis/argoproj.io/v1alpha1/workflows",
            None,
            id="proxied-cluster",
        ),
    ],
)
def test_workflow_listing_is_get_only_for_local_and_proxied_clusters(
    cluster, expected_url, expected_headers, tmp_path, monkeypatch
):
    """The local SA and proxy transports must share the read-only contract.

    A get-only transport makes an accidental POST/PUT/PATCH/DELETE (or a
    watch-specific request method) fail immediately, while the assertions pin
    the collection-list query shape to only the parameters supported by the
    get/list RBAC grant.
    """
    if cluster.base_url is None:
        token_path = tmp_path / "token"
        token_path.write_text("sa-token\n")
        ca_path = tmp_path / "ca.crt"
        ca_path.write_text("certificate")
        monkeypatch.setattr(k8s_api, "_SA_TOKEN_PATH", str(token_path))
        monkeypatch.setattr(k8s_api, "_SA_CA_PATH", str(ca_path))

    class GetOnlyRequests:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, **kwargs):
            params = dict(params or {})
            self.calls.append((url, params, kwargs))
            metadata = {"continue": "next-page"} if len(self.calls) == 1 else {}
            return SimpleNamespace(
                status_code=200,
                json=lambda: {"items": [{"name": f"page-{len(self.calls)}"}], "metadata": metadata},
            )

    transport = GetOnlyRequests()
    monkeypatch.setattr(k8s_api, "requests", transport)

    items, complete = list_items(
        cluster,
        "/apis/argoproj.io/v1alpha1/workflows",
        timeout=17,
        page_size=2,
    )

    assert complete is True
    assert items == [{"name": "page-1"}, {"name": "page-2"}]
    assert transport.calls == [
        (
            expected_url,
            {"limit": 2},
            {
                **({"headers": expected_headers, "verify": str(tmp_path / "ca.crt")}
                   if expected_headers
                   else {}),
                "timeout": 17,
            },
        ),
        (
            expected_url,
            {"limit": 2, "continue": "next-page"},
            {
                **({"headers": expected_headers, "verify": str(tmp_path / "ca.crt")}
                   if expected_headers
                   else {}),
                "timeout": 17,
            },
        ),
    ]


def test_fetch_json_returns_none_when_the_request_times_out(monkeypatch):
    def fake_get(*args, **kwargs):
        raise k8s_api.requests.Timeout("request timed out")

    monkeypatch.setattr(k8s_api.requests, "get", fake_get)
    assert fetch_json(_CLUSTER, "/p", 17) is None


def test_paging_never_sends_a_watch_parameter():
    """Every page request is a plain list call: `limit` and, after the first
    page, `continue` — nothing else."""
    fetch, calls = _pages(
        {"items": [], "metadata": {"continue": "tok"}},
        {"items": [], "metadata": {}},
    )
    list_items(_CLUSTER, "/p", 10, 500, fetch=fetch)

    assert calls
    for params in calls:
        assert "watch" not in params
        assert set(params) <= {"limit", "continue"}
