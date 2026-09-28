"""Contract tests for the Forgejo-to-Argo release handoff.

The release WorkflowTemplate and Argo Events Sensor live in the sibling
``declarative-config`` repository in production.  These small fixtures keep
the contract executable from an application-only Forgejo clone, which is the
checkout that the build workflow tests before it resolves a release version.
"""

import os
import re
import shlex
import subprocess
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures"


def _load_fixture(name: str) -> dict:
    return yaml.safe_load((FIXTURES / name).read_text(encoding="utf-8"))


def _workflow() -> dict:
    return _load_fixture("argo-workflows-exporter-workflow.yml")


def _sensor() -> dict:
    return _load_fixture("argo-workflows-exporter-sensor.yml")


def _deployment() -> dict:
    return _load_fixture("argo-workflows-exporter-deployment.yml")


def _templates(workflow: dict) -> dict[str, dict]:
    return {template["name"]: template for template in workflow["spec"]["templates"]}


def _parameters(resource: dict) -> dict[str, str]:
    return {
        parameter["name"]: parameter["value"]
        for parameter in resource["spec"]["arguments"]["parameters"]
    }


def _git(cwd: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _new_bare_repository(root: Path, name: str) -> tuple[Path, Path]:
    origin = root / f"{name}.git"
    seed = root / f"{name}-seed"
    _git(root, "init", "--bare", "--initial-branch=main", str(origin))
    _git(root, "init", "--initial-branch=main", str(seed))
    _git(seed, "config", "user.name", "Release test")
    _git(seed, "config", "user.email", "release-test@example.invalid")
    _git(seed, "remote", "add", "origin", str(origin))
    return origin, seed


def _application_origin(root: Path, *, explicit_version_change: bool) -> Path:
    origin, seed = _new_bare_repository(root, "application")
    (seed / "VERSION").write_text("1.2.3\n", encoding="utf-8")
    (seed / "release-input.txt").write_text("initial\n", encoding="utf-8")
    _git(seed, "add", "VERSION", "release-input.txt")
    _git(seed, "commit", "-m", "initial release state")

    if explicit_version_change:
        (seed / "VERSION").write_text("1.2.9\n", encoding="utf-8")
    else:
        (seed / "release-input.txt").write_text("application change\n", encoding="utf-8")
    _git(seed, "add", "VERSION", "release-input.txt")
    _git(seed, "commit", "-m", "trigger release")
    _git(seed, "push", "--set-upstream", "origin", "main")
    return origin


def _run_resolve_version(
    workflow: dict, root: Path, origin: Path, output_path: Path
) -> str:
    """Run the WorkflowTemplate's resolver against a local bare Git repo."""
    script = _templates(workflow)["resolve-version"]["script"]["source"]
    checkout = root / "resolve-checkout"
    clone_pattern = re.compile(
        r'git clone --branch \{\{workflow\.parameters\.branch\}\} \\\n'
        r'\s+"https://git\.ardenone\.com/\{\{workflow\.parameters\.git-repo\}\}\.git" \\\n'
        r'\s+/tmp/repo'
    )
    script, replacements = clone_pattern.subn(
        "git clone --branch main "
        f"{shlex.quote(str(origin))} {shlex.quote(str(checkout))}",
        script,
        count=1,
    )
    assert replacements == 1, script
    script = script.replace("/tmp/repo", str(checkout))
    script = script.replace("/tmp/version", str(output_path))

    result = subprocess.run(
        ["sh", "-c", script],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    assert result.returncode == 0, result.stderr
    return output_path.read_text(encoding="utf-8").strip()


def test_release_workflow_runs_tests_before_resolving_and_building():
    workflow = _workflow()
    templates = _templates(workflow)
    steps = workflow["spec"]["templates"][0]["steps"]

    assert workflow["spec"]["entrypoint"] == "build"
    assert _parameters(workflow) == {
        "git-repo": "jedarden/argo-workflows-exporter",
        "branch": "main",
    }
    assert [group[0]["name"] for group in steps] == [
        "test",
        "resolve-version",
        "docker-build",
    ]
    assert "python -m pytest tests/ -q" in templates["test"]["script"]["source"]
    assert steps[2][0]["arguments"]["parameters"] == [
        {
            "name": "version",
            "value": "{{steps.resolve-version.outputs.parameters.version}}",
        }
    ]


@pytest.mark.parametrize(
    "explicit_version_change, expected_version",
    [(False, "1.2.4"), (True, "1.2.9")],
    ids=["automatic-version-bump", "explicit-version-change"],
)
def test_resolver_honors_explicit_versions_and_pushes_patch_bumps(
    tmp_path: Path, explicit_version_change: bool, expected_version: str
):
    """Exercise both resolver branches against the real resolver shell source."""
    origin = _application_origin(
        tmp_path, explicit_version_change=explicit_version_change
    )
    resolved = _run_resolve_version(
        _workflow(), tmp_path, origin, tmp_path / "resolved-version"
    )

    assert resolved == expected_version
    assert _git(tmp_path, "--git-dir", str(origin), "show", "main:VERSION") == resolved

    latest_subject = _git(
        tmp_path, "--git-dir", str(origin), "log", "-1", "--format=%s", "main"
    )
    if explicit_version_change:
        assert latest_subject == "trigger release"
    else:
        assert latest_subject == f"ci: auto-bump version to {resolved}"
        assert _git(
            tmp_path,
            "--git-dir",
            str(origin),
            "show",
            "--format=",
            "--name-only",
            "main",
        ).splitlines() == ["VERSION"]
        assert _git(
            tmp_path,
            "--git-dir",
            str(origin),
            "show",
            "-s",
            "--format=%an",
            "main",
        ) == "Argo Workflows CI"


def test_ci_writeback_author_is_excluded_from_the_build_sensor():
    sensor = _sensor()
    dependency = sensor["spec"]["dependencies"][0]
    filters = dependency["filters"]["data"]
    author_filter = next(
        item for item in filters if item["path"] == "body.head_commit.author.name"
    )

    assert author_filter["comparator"] == "!="
    assert author_filter["value"] == ["Argo Workflows CI"]
    assert dependency["eventSourceName"] == "github-webhooks"
    assert dependency["eventName"] == "argo-workflows-exporter"

    trigger_resource = sensor["spec"]["triggers"][0]["template"]["argoWorkflow"][
        "source"
    ]["resource"]
    assert _parameters(trigger_resource) == {
        "git-repo": "jedarden/argo-workflows-exporter",
        "branch": "main",
    }

    resolve_source = _templates(_workflow())["resolve-version"]["script"]["source"]
    assert 'git config user.email "github@jedarden.com"' in resolve_source
    assert 'git config user.name "Argo Workflows CI"' in resolve_source


def test_kaniko_receives_the_single_resolved_semver_for_tag_and_build_arg():
    workflow = _workflow()
    templates = _templates(workflow)
    docker_args = templates["docker-build"]["container"]["args"]
    build_steps = templates["build"]["steps"]

    assert build_steps[2][0]["arguments"]["parameters"][0]["value"] == (
        "{{steps.resolve-version.outputs.parameters.version}}"
    )
    assert (
        "--destination=ronaldraygun/argo-workflows-exporter:"
        "{{inputs.parameters.version}}"
    ) in docker_args
    assert "--build-arg=VERSION={{inputs.parameters.version}}" in docker_args
    assert (
        "--context=git://git.ardenone.com/{{workflow.parameters.git-repo}}.git"
        "#refs/heads/{{workflow.parameters.branch}}"
    ) in docker_args

    resolve_source = templates["resolve-version"]["script"]["source"]
    assert 'echo "$VERSION" > /tmp/version' in resolve_source
    assert "COPY VERSION ." in (ROOT / "Dockerfile").read_text(encoding="utf-8")


def test_production_promotion_is_separate_and_uses_an_immutable_image_tag():
    workflow = _workflow()
    workflow_text = (FIXTURES / "argo-workflows-exporter-workflow.yml").read_text(
        encoding="utf-8"
    )
    image = _deployment()["spec"]["template"]["spec"]["containers"][0]["image"]

    assert re.fullmatch(r"ronaldraygun/argo-workflows-exporter:\d+\.\d+\.\d+", image)
    assert not image.endswith(":latest")
    assert "promote" not in {
        template["name"] for template in workflow["spec"]["templates"]
    }
    assert "declarative-config" not in workflow_text
    assert "deployment.yml" not in workflow_text
    assert "kubectl" not in workflow_text
