#!/usr/bin/env python3
"""Minimal standard-library producer for scala-slurm result envelope v1."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import argparse
from pathlib import Path
from typing import Any


def sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def output_entry(root: Path, relative: str) -> dict[str, Any]:
    path = root / relative
    data = path.read_bytes()
    return {"path": relative, "sizeBytes": len(data), "digest": sha256(data)}


def encode_envelope(payload: dict[str, Any]) -> bytes:
    document = {
        "protocol": {"major": 1, "minor": 0},
        "schema": "scala-slurm.result-envelope",
        "payload": payload,
    }
    return (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()


def publish_atomic(target: Path, document: bytes) -> None:
    if target.exists():
        raise FileExistsError(f"result already exists: {target}")
    temporary = target.with_name(f".{target.name}.tmp-{os.getpid()}")
    with temporary.open("xb") as stream:
        stream.write(document)
        stream.flush()
        os.fsync(stream.fileno())
    os.rename(temporary, target)


def succeeded_payload(
    *,
    submission_key: str,
    attempt_id: str,
    attempt_epoch: int,
    operation: dict[str, Any],
    result_schema: str,
    value: bytes,
    outputs: list[dict[str, Any]],
    worker_release: dict[str, str],
    completed_at: str,
) -> dict[str, Any]:
    return {
        "submissionKey": submission_key,
        "attemptId": attempt_id,
        "attemptEpoch": attempt_epoch,
        "job": None,
        "operation": operation,
        "resultSchema": result_schema,
        "status": {"kind": "succeeded"},
        "valueBase64": base64.b64encode(value).decode("ascii"),
        "outputs": outputs,
        "workerRelease": worker_release,
        "completedAt": completed_at,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("workspace", type=Path)
    args = parser.parse_args()
    output = args.workspace / "results" / "data.csv"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b"x,y\n1,2\n")
    payload = succeeded_payload(
        submission_key="python-example",
        attempt_id="managed-python-example",
        attempt_epoch=1,
        operation={
            "kind": "script",
            "sourceDigest": "sha256:" + "a" * 64,
        },
        result_schema="example.python.result.v1",
        value=b'{"answer":42}',
        outputs=[output_entry(args.workspace, "results/data.csv")],
        worker_release={
            "id": "python-helper-1",
            "digest": "sha256:de8d84121ae5491ab72622c895ebaf1cefe295a4ce360477d4730f94d07feb66",
        },
        completed_at="2026-07-22T12:00:00Z",
    )
    publish_atomic(args.workspace / "result.json", encode_envelope(payload))


if __name__ == "__main__":
    main()
