from src.workflows import duration_seconds, failed_step, template_of, to_row, trigger_of


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
