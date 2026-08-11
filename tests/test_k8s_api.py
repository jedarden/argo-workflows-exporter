from src.config import Cluster
from src.k8s_api import list_items, list_path

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
