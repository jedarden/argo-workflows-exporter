from types import SimpleNamespace

from src.config import Cluster
from src import k8s_api
from src.k8s_api import fetch_json, list_items, list_path

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
    assert list_path("argo", "workflows") == "/apis/argoproj.io/v1alpha1/namespaces/argo/workflows"


def test_single_page_listing():
    fetch, _ = _pages({"items": [{"a": 1}], "metadata": {}})
    items, complete = list_items(_CLUSTER, "/p", 10, 500, fetch=fetch)
    assert (items, complete) == ([{"a": 1}], True)


def test_paging_follows_the_continue_token():
    fetch, calls = _pages(
        {"items": [{"a": 1}], "metadata": {"continue": "tok"}},
        {"items": [{"a": 2}], "metadata": {}},
    )
    items, complete = list_items(_CLUSTER, "/p", 10, 500, fetch=fetch)

    assert complete is True
    assert items == [{"a": 1}, {"a": 2}]
    assert calls[0] == {"limit": 500}
    assert calls[1] == {"limit": 500, "continue": "tok"}


def test_a_failed_page_marks_the_listing_incomplete():
    """A truncated list would read as workflows having been deleted."""
    fetch, _ = _pages({"items": [{"a": 1}], "metadata": {"continue": "tok"}}, None)
    items, complete = list_items(_CLUSTER, "/p", 10, 500, fetch=fetch)
    assert complete is False
    assert items == [{"a": 1}]


def test_first_page_failure_is_incomplete_and_empty():
    fetch, _ = _pages(None)
    assert list_items(_CLUSTER, "/p", 10, 500, fetch=fetch) == ([], False)


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
