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
IMAGE_REPOSITORY = "ronaldraygun/argo-workflows-exporter"
DEPLOYMENT_PATH = Path("k8s/ardenone-cluster/argo-workflows-exporter/deployment.yml")
SEMVER_TAG = re.compile(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)")


def _load_fixture(name: str) -> dict:
    return yaml.safe_load((FIXTURES / name).read_text(encoding="utf-8"))


def _workflow() -> dict:
    return _load_fixture("argo-workflows-exporter-workflow.yml")


def _sensor() -> dict:
    return _load_fixture("argo-workflows-exporter-sensor.yml")


def _deployment() -> dict:
    return _load_fixture("argo-workflows-exporter-deployment.yml")


def _deployment_image(deployment: dict) -> str:
    return deployment["spec"]["template"]["spec"]["containers"][0]["image"]


def _set_deployment_image(path: Path, image: str) -> None:
    deployment = yaml.safe_load(path.read_text(encoding="utf-8"))
    deployment["spec"]["template"]["spec"]["containers"][0]["image"] = image
    path.write_text(yaml.safe_dump(deployment, sort_keys=False), encoding="utf-8")


def _assert_immutable_semver_image(image: str) -> str:
    match = re.fullmatch(
        rf"{re.escape(IMAGE_REPOSITORY)}:(?P<tag>[^:@]+)", image
    )
    assert match, f"production image must use a tag: {image}"
    tag = match.group("tag")
    assert SEMVER_TAG.fullmatch(tag), f"production image must use semver: {image}"
    return tag


def _promotion_image(build: dict) -> str:
    """Return the only image reference the production promotion may use."""
    assert build["status"] == "Succeeded", "promotion waits for a successful build"
    tag = build["published_tag"]
    assert tag in build["published_tags"], "promotion requires the published tag"
    image = f"{IMAGE_REPOSITORY}:{tag}"
    _assert_immutable_semver_image(image)
    return image


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


def _sensor_accepts_push(sensor: dict, *, author_name: str) -> bool:
    """Apply the fixture's push filters to the small event shape they use."""
    event = {
        "headers": {"X-Github-Event": "push"},
        "body": {
            "ref": "refs/heads/main",
            "head_commit": {"author": {"name": author_name}},
        },
    }
    dependency = sensor["spec"]["dependencies"][0]
    for data_filter in dependency["filters"]["data"]:
        observed = event
        for path_part in data_filter["path"].split("."):
            observed = observed[path_part]
        expected = data_filter["value"]
        if data_filter.get("comparator") == "!=":
            if observed in expected:
                return False
        elif observed not in expected:
            return False
    return True


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
        },
        {
            "name": "source-sha",
            "value": "{{steps.resolve-version.outputs.parameters.source-sha}}",
        },
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


@pytest.mark.parametrize(
    "explicit_version_change",
    [False, True],
    ids=["source-without-version-change", "source-with-explicit-version-change"],
)
def test_ci_version_writeback_does_not_start_a_second_build(
    tmp_path: Path, explicit_version_change: bool
):
    """A release source push produces one sensor-accepted build in either mode."""
    origin = _application_origin(
        tmp_path, explicit_version_change=explicit_version_change
    )
    sensor = _sensor()
    source_revision = _git(tmp_path, "--git-dir", str(origin), "rev-parse", "main")
    source_author = _git(
        tmp_path,
        "--git-dir",
        str(origin),
        "show",
        "-s",
        "--format=%an",
        "main",
    )

    assert _sensor_accepts_push(sensor, author_name=source_author)
    assert not _sensor_accepts_push(sensor, author_name="Argo Workflows CI")

    _run_resolve_version(
        _workflow(), tmp_path, origin, tmp_path / "resolved-version"
    )

    latest_revision = _git(tmp_path, "--git-dir", str(origin), "rev-parse", "main")
    latest_author = _git(
        tmp_path,
        "--git-dir",
        str(origin),
        "show",
        "-s",
        "--format=%an",
        "main",
    )
    if explicit_version_change:
        assert latest_revision == source_revision
        push_authors = [source_author]
    else:
        assert latest_revision != source_revision
        assert latest_author == "Argo Workflows CI"
        push_authors = [source_author, latest_author]

    accepted_build_authors = [
        author
        for author in push_authors
        if _sensor_accepts_push(sensor, author_name=author)
    ]
    assert accepted_build_authors == [source_author]


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


def test_buildkit_receives_the_single_resolved_semver_and_source_sha():
    workflow = _workflow()
    templates = _templates(workflow)
    docker = templates["docker-build"]
    build_source = docker["container"]["args"][0]
    build_steps = templates["build"]["steps"]

    assert build_steps[2][0]["arguments"]["parameters"][0]["value"] == (
        "{{steps.resolve-version.outputs.parameters.version}}"
    )
    assert build_steps[2][0]["arguments"]["parameters"][1]["value"] == (
        "{{steps.resolve-version.outputs.parameters.source-sha}}"
    )
    assert (
        "type=image,name=ronaldraygun/argo-workflows-exporter:"
        "{{inputs.parameters.version}},push=true"
    ) in build_source
    assert '--opt "build-arg:VERSION={{inputs.parameters.version}}"' in build_source
    assert "#{{inputs.parameters.source-sha}}" in build_source
    assert docker["metadata"]["labels"]["ci.ardenone.com/buildkit-client"] == "true"
    assert docker["container"]["resources"]["requests"]["ephemeral-storage"] == "64Mi"

    resolve_source = templates["resolve-version"]["script"]["source"]
    assert 'echo "$VERSION" > /tmp/version' in resolve_source
    assert "git rev-parse HEAD > /tmp/source-sha" in resolve_source
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


def test_production_upgrade_and_rollback_use_only_successful_immutable_tags(
    tmp_path: Path,
):
    """Exercise the runbook's GitOps promotion and rollback sequence."""
    config_repo = tmp_path / "declarative-config"
    deployment_path = config_repo / DEPLOYMENT_PATH
    deployment_path.parent.mkdir(parents=True)
    deployment_path.write_text(
        (FIXTURES / "argo-workflows-exporter-deployment.yml").read_text(
            encoding="utf-8"
        ),
        encoding="utf-8",
    )
    _git(tmp_path, "init", "--initial-branch=main", str(config_repo))
    _git(config_repo, "config", "user.name", "Release test")
    _git(config_repo, "config", "user.email", "release-test@example.invalid")
    _git(config_repo, "add", str(DEPLOYMENT_PATH))
    _git(config_repo, "commit", "-m", "production baseline")

    baseline_image = _deployment_image(
        yaml.safe_load(deployment_path.read_text(encoding="utf-8"))
    )
    baseline_tag = _assert_immutable_semver_image(baseline_image)

    # These fallback-shaped fields model values that are available to the
    # release workflow but are not valid production image selectors.
    build = {
        "status": "Succeeded",
        "published_tag": "0.2.66",
        "published_tags": ["0.2.66"],
        "version_file": "0.2.64",
        "source_revision": "a" * 40,
    }
    promoted_image = _promotion_image(build)
    promoted_tag = _assert_immutable_semver_image(promoted_image)
    assert promoted_tag == build["published_tag"]
    assert promoted_tag != build["version_file"]
    assert promoted_tag != build["source_revision"]

    _set_deployment_image(deployment_path, promoted_image)
    _git(config_repo, "add", str(DEPLOYMENT_PATH))
    _git(config_repo, "commit", "-m", f"promote exporter to {promoted_tag}")
    assert _deployment_image(
        yaml.safe_load(deployment_path.read_text(encoding="utf-8"))
    ) == promoted_image
    assert _git(
        config_repo, "show", "--format=", "--name-only", "HEAD"
    ).splitlines() == [str(DEPLOYMENT_PATH)]

    # Rollback derives the target from the previous GitOps deployment commit,
    # never from VERSION, a source revision, or a floating image tag.
    previous_deployment = yaml.safe_load(
        _git(config_repo, "show", f"HEAD~1:{DEPLOYMENT_PATH}")
    )
    rollback_image = _deployment_image(previous_deployment)
    assert _assert_immutable_semver_image(rollback_image) == baseline_tag
    assert rollback_image == baseline_image

    _set_deployment_image(deployment_path, rollback_image)
    _git(config_repo, "add", str(DEPLOYMENT_PATH))
    _git(config_repo, "commit", "-m", f"rollback exporter to {baseline_tag}")
    assert _deployment_image(
        yaml.safe_load(deployment_path.read_text(encoding="utf-8"))
    ) == baseline_image
    assert _git(
        config_repo, "show", "--format=", "--name-only", "HEAD"
    ).splitlines() == [str(DEPLOYMENT_PATH)]


@pytest.mark.parametrize(
    "published_tag",
    [
        "VERSION",
        "latest",
        "a" * 40,
    ],
    ids=["version-fallback", "floating-tag", "commit-id-fallback"],
)
def test_production_promotion_rejects_non_semver_release_selectors(
    published_tag: str,
):
    build = {
        "status": "Succeeded",
        "published_tag": published_tag,
        "published_tags": [published_tag],
    }

    with pytest.raises(AssertionError):
        _promotion_image(build)


def test_production_promotion_rejects_an_unsuccessful_build():
    build = {
        "status": "Failed",
        "published_tag": "0.2.66",
        "published_tags": ["0.2.66"],
    }

    with pytest.raises(AssertionError):
        _promotion_image(build)
