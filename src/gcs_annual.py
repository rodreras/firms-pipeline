"""Replace fixed annual paths using generation checks and restore on handled failures."""

import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from src.dataset import GCSUploadError, cloud_uri, sha256, upload_dataset
from src.logs import event


def current_generation(uri: str, path: Path) -> str:
    result = subprocess.run([
        "gcloud", "storage", "objects", "describe", uri, "--raw", "--format=value(generation)",
    ], capture_output=True, text=True)
    if result.returncode == 0 and result.stdout.strip().isdecimal():
        return result.stdout.strip()
    if result.returncode and any(mark in result.stderr.lower() for mark in (
        "404", "matched no objects", "no urls matched", "not found",
    )):
        return "0"
    raise GCSUploadError(path, "annual_generation_read_failed", result.stderr.strip())


def copy_generation(uri: str, generation: str, target: Path, path: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run([
        "gcloud", "storage", "cp", f"{uri}#{generation}", str(target),
    ], capture_output=True, text=True)
    if result.returncode:
        raise GCSUploadError(path, "annual_backup_failed", result.stderr.strip())


def replace_object(local: Path, uri: str, generation: str, path: Path) -> None:
    event("annual_upload_started", uri=uri, generation=generation)
    result = subprocess.run([
        "gcloud", "storage", "cp", f"--if-generation-match={generation}",
        f"--custom-metadata=sha256={sha256(local)}", str(local), uri,
    ], capture_output=True, text=True)
    if result.returncode:
        raise GCSUploadError(path, "annual_write_failed", result.stderr.strip())
    event("annual_upload_finished", uri=uri)


@contextmanager
def update_annual_objects(
    output: Path, destination: str, paths: list[Path], expected: dict[Path, str],
    *, backup_destination: str,
) -> Iterator[None]:
    backup = output.parent / "annual-backup"
    pending = []
    for path in paths:
        uri = cloud_uri(destination, path)
        generation = current_generation(uri, path)
        old = backup / path
        local = output / path
        if generation != "0":
            copy_generation(uri, generation, old, path)
            prior_digest = sha256(old)
            if path in expected and prior_digest != expected[path]:
                raise GCSUploadError(path, "annual_base_changed", "the published bytes changed; rerun after checking the Item")
            if prior_digest == sha256(local):
                continue
        elif path in expected:
            raise GCSUploadError(path, "annual_base_missing", "the published object disappeared")
        pending.append((path, uri, generation, old))

    previous_paths = [path for path, _, generation, _ in pending if generation != "0"]
    if previous_paths:
        upload_dataset(backup, backup_destination, previous_paths)
    written = []
    try:
        for path, uri, generation, old in pending:
            replace_object(output / path, uri, generation, path)
            if generation != "0":
                written.append((path, uri, old))
        yield
    except (Exception, KeyboardInterrupt) as error:
        for path, uri, old in reversed(written):
            try:
                current = current_generation(uri, path)
                check = backup / "written" / path
                copy_generation(uri, current, check, path)
                if sha256(check) != sha256(output / path):
                    raise GCSUploadError(path, "annual_rollback_conflict", "another writer changed this object")
                replace_object(old, uri, current, path)
            except GCSUploadError as rollback:
                event("annual_rollback_failed", level="ERROR", object=path.as_posix(), reason=rollback.reason)
                error.add_note(f"Annual rollback failed for {path.as_posix()}.")
        raise
