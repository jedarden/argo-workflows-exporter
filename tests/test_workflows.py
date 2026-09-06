import re

import pytest

from src.workflows import (
    _load_failure_classes,
    duration_seconds,
    failed_step,
    failure_class,
    normalize_failure,
    template_of,
    to_row,
    trigger_of,
)


def _wf(**overrides):
    wf = {
        "metadata": {
            "uid": "uid-1",
            "name": "example-build-abcde",
            "namespace": "argo",
            "creationTimestamp": "2026-08-11T03:31:30Z",
            "labels": {},
        },
        "spec": {},
        "status": {"phase": "Succeeded", "startedAt": "2026-08-11T03:31:31Z"},
    }
    for key, value in overrides.items():
        wf[key] = value
    return wf


def test_template_prefers_the_spec_reference():
    wf = _wf(spec={"workflowTemplateRef": {"name": "example-build"}})
    wf["metadata"]["labels"] = {"workflows.argoproj.io/workflow-template": "stale-label"}
    assert template_of(wf) == ("example-build", "namespaced")


def test_template_marks_cluster_scope():
    wf = _wf(spec={"workflowTemplateRef": {"name": "shared", "clusterScope": True}})
    assert template_of(wf) == ("shared", "cluster")


def test_template_falls_back_to_the_label():
    wf = _wf()
    wf["metadata"]["labels"] = {"workflows.argoproj.io/workflow-template": "labelled"}
    assert template_of(wf) == ("labelled", "namespaced")


def test_inline_workflow_has_no_template_rather_than_a_guessed_one():
    assert template_of(_wf(spec={"templates": [{"name": "main"}]})) == (None, None)


def test_trigger_kinds():
    cron = _wf()
    cron["metadata"]["labels"] = {"workflows.argoproj.io/cron-workflow": "nightly"}
    assert trigger_of(cron) == ("cron", "nightly")

    event = _wf()
    event["metadata"]["labels"] = {
        "events.argoproj.io/sensor": "repo-sensor",
        "events.argoproj.io/trigger": "example-build",
        "workflows.argoproj.io/creator": "system-serviceaccount-argo-events-default",
    }
    assert trigger_of(event) == ("event", "example-build")

    user = _wf()
    user["metadata"]["labels"] = {"workflows.argoproj.io/creator": "someone"}
    assert trigger_of(user) == ("user", "someone")

    assert trigger_of(_wf()) == (None, None)


def test_event_trigger_falls_back_to_the_sensor_name():
    wf = _wf()
    wf["metadata"]["labels"] = {"events.argoproj.io/sensor": "repo-sensor"}
    assert trigger_of(wf) == ("event", "repo-sensor")


def test_duration_is_none_while_running():
    assert duration_seconds("2026-08-11T03:31:31Z", None) is None
    assert duration_seconds(None, None) is None
    assert duration_seconds("2026-08-11T03:31:31Z", "2026-08-11T03:32:33Z") == 62


def test_failed_step_reports_the_earliest_failing_pod_not_its_parents():
    wf = _wf(
        status={
            "phase": "Failed",
            "nodes": {
                "a": {"type": "Steps", "phase": "Failed", "displayName": "whole-workflow"},
                "b": {
                    "type": "Pod", "phase": "Failed", "displayName": "test",
                    "startedAt": "2026-08-11T03:40:00Z", "message": "exit code 1",
                },
                "c": {
                    "type": "Pod", "phase": "Error", "displayName": "publish",
                    "startedAt": "2026-08-11T03:45:00Z", "message": "later, consequential",
                },
            },
        }
    )
    assert failed_step(wf) == ("test", "exit code 1")


def test_failed_step_absent_when_nodes_are_compressed():
    wf = _wf(status={"phase": "Failed", "compressedNodes": "H4sIA..."})
    assert failed_step(wf) == (None, None)


def test_unadmitted_workflow_reports_pending_rather_than_an_empty_phase():
    row = to_row(_wf(status={}), "ci", "2026-08-11T04:00:00Z")
    assert row["phase"] == "Pending"


def test_to_row_shape():
    wf = _wf(
        spec={"workflowTemplateRef": {"name": "example-build"}},
        status={
            "phase": "Succeeded",
            "progress": "2/2",
            "startedAt": "2026-08-11T03:31:31Z",
            "finishedAt": "2026-08-11T03:32:33Z",
            "resourcesDuration": {"cpu": 31, "memory": 605, "nvidia.com/gpu": 7},
        },
    )
    row = to_row(wf, "ci", "2026-08-11T04:00:00Z")
    assert row["uid"] == "uid-1"
    assert row["cluster"] == "ci"
    assert row["template"] == "example-build"
    assert row["duration_seconds"] == 62
    assert row["resources_duration_cpu"] == 31
    assert row["resources_duration_memory"] == 605
    assert row["observed_at"] == "2026-08-11T04:00:00Z"
    # Extended resources are not kept as columns.
    assert "nvidia.com/gpu" not in row


# Real messages, in the shape Argo, Kubernetes and the tooling behind a step
# actually emit them. Every class in `failure_classes.yaml` must be reached by
# at least one of these; a rule added to that file without a message here is a
# rule nothing notices breaking.
FAILURE_MESSAGES = [
    # timeout
    ("Pod was active on the node longer than the specified deadline", "timeout"),
    ("Step 'build' hit its 20m0s deadline: context deadline exceeded", "timeout"),
    ("Timed out after 300.00s waiting for pods to become ready", "timeout"),
    # oom
    ("Last State: Terminated, Reason: OOMKilled, Exit Code: 137", "oom"),
    ("fatal error: runtime: out of memory", "oom"),
    # clone_auth
    ("failed to clone repository: authentication required for "
     "https://git.ardenone.com/jedarden/perch.git", "clone_auth"),
    ("fatal: could not read Username for 'https://git.ardenone.com': "
     "terminal prompts disabled", "clone_auth"),
    # image_pull
    ("Failed to pull image \"ronaldraygun/spaxel:1.4.2\": pull access denied "
     "for spaxel, repository does not exist or may require authorization", "image_pull"),
    ("Back-off pulling image \"ronaldraygun/vista:0.9.0\": manifest unknown", "image_pull"),
    # test_failure
    ("FAILED tests/test_export.py::test_roundtrip - assert 4 == 5", "test_failure"),
    ("--- FAIL: TestParseDuration (0.00s)\n    parse_test.go:22: got 62, want 3600", "test_failure"),
    # lint
    ("ruff check failed: F401 'os' imported but unused (3 errors)", "lint"),
    ("error: clippy::needless_borrow on src/ledger.rs:88", "lint"),
    # build
    ("error[E0432]: unresolved import crate::workflows", "build"),
    ("npm ERR! code ELIFECYCLE\nnpm ERR! errno 1", "build"),
    # infrastructure
    ("0/3 nodes are available: 2 Insufficient cpu, 1 node(s) didn't match "
     "Pod's node affinity", "infrastructure"),
    ("Get \"https://kubernetes.default.svc\": dial tcp 10.96.0.1:443: "
     "connection refused", "infrastructure"),
    # unknown
    ("something unexpected went wrong", "unknown"),
    ("invalid spec: templates.main.arguments.value is required", "unknown"),
]

FAILURE_CLASSES = {
    "timeout", "oom", "clone_auth", "image_pull",
    "test_failure", "lint", "build", "infrastructure", "unknown",
}


@pytest.mark.parametrize("message,expected", FAILURE_MESSAGES)
def test_failure_class_of_a_real_message(message, expected):
    assert failure_class(message) == expected


def test_failure_class_table_reaches_every_class():
    """One message per class guards the table against a rule that has drifted
    out of reach — the commonest way a taxonomy rots is silently."""
    seen = {cls for _, cls in FAILURE_MESSAGES}
    assert seen == FAILURE_CLASSES


def test_failure_class_without_a_message_is_null():
    assert failure_class(None) is None
    assert failure_class("") is None
    assert failure_class("   ") is None


@pytest.mark.parametrize("message,expected", FAILURE_MESSAGES)
def test_failure_fingerprint_is_a_short_stable_hash(message, expected):
    _, fingerprint = normalize_failure(message)
    assert fingerprint == normalize_failure(message)[1]
    assert re.fullmatch(r"[0-9a-f]{12}", fingerprint)


def test_fingerprint_ignores_instance_specific_tokens():
    """The same failure twice, distinguished only by the pod it landed on, the
    path it ran in and the moment it happened, is one failure for grouping."""
    first, _ = normalize_failure(
        "Pod rust-verify-7gk2m failed: exit code 1 at 2026-09-06T03:31:30Z "
        "log /var/log/argo/pods/rust-verify-7gk2m/main.log"
    )
    second, same = normalize_failure(
        "Pod rust-verify-9d4p1 failed: exit code 1 at 2026-09-07T14:02:11Z "
        "log /var/log/argo/pods/rust-verify-9d4p1/main.log"
    )
    assert same == normalize_failure(
        "pod rust-verify-7gk2m failed: exit code 1 at 2026-09-06t03:31:30z "
        "log /var/log/argo/pods/rust-verify-7gk2m/main.log"
    )[1]
    assert "rust-verify" not in first and "2026" not in first
    assert first == second


def test_fingerprint_keeps_different_failures_apart():
    fingerprints = {normalize_failure(m)[1] for m, _ in FAILURE_MESSAGES}
    # 20 messages, several of which share a class: distinctness across every
    # one of them is a stronger statement than pairwise-per-class checks, and
    # these messages are different failures.
    assert len(fingerprints) == len(FAILURE_MESSAGES)


def test_normalization_uses_placeholders_and_collapses_whitespace():
    text, _ = normalize_failure(
        "Build d6e0715  failed\nafter 1h2m3s:\t/home/coding/repo/src/main.py:42"
    )
    assert text == "build <hash> failed after <dur>: <path>"


def test_normalization_leaves_ordinary_prose_alone():
    """Hex-flavoured words and hand-written hyphenated names are not ids."""
    text, _ = normalize_failure("The feedback loop was not deadbeef after all")
    assert text == "the feedback loop was not deadbeef after all"
    assert normalize_failure("argo-workflows-exporter failed")[0] == \
        "argo-workflows-exporter failed"


def test_normalize_failure_without_a_message_is_null():
    assert normalize_failure(None) == (None, None)
    assert normalize_failure("  ") == (None, None)


def test_rule_table_rejects_unknown_as_an_explicit_rule(tmp_path):
    """`unknown` is the fallback, not a rule; listing it would shadow every
    rule below it."""
    rulebook = tmp_path / "failure_classes.yaml"
    rulebook.write_text(
        "- class: unknown\n  patterns:\n    - 'nope'\n"
        "- class: build\n  patterns:\n    - 'build failed'\n"
    )
    with pytest.raises(ValueError, match="fallback"):
        _load_failure_classes(rulebook)


def test_to_row_derives_failure_columns_from_the_step_message():
    wf = _wf(
        status={
            "phase": "Failed",
            "message": "failed step 'test'",
            "nodes": {
                "b": {
                    "type": "Pod", "phase": "Failed", "displayName": "test",
                    "startedAt": "2026-08-11T03:40:00Z",
                    "message": "error[E0432]: unresolved import crate::workflows",
                },
            },
        }
    )
    row = to_row(wf, "ci", "2026-08-11T04:00:00Z")
    assert row["failure_class"] == "build"
    _, fingerprint = normalize_failure("error[E0432]: unresolved import crate::workflows")
    assert row["failure_fingerprint"] == fingerprint


def test_to_row_falls_back_to_the_workflow_message():
    """A run whose nodes were compressed still has `message` to classify."""
    wf = _wf(status={"phase": "Failed", "message": "Pod was active on the node "
                                                 "longer than the specified deadline"})
    row = to_row(wf, "ci", "2026-08-11T04:00:00Z")
    assert row["failure_class"] == "timeout"
    assert row["failed_step_message"] is None
    assert row["failure_fingerprint"]


def test_to_row_leaves_failure_columns_null_for_a_clean_run():
    row = to_row(_wf(), "ci", "2026-08-11T04:00:00Z")
    assert row["failure_fingerprint"] is None
    assert row["failure_class"] is None
