#!/usr/bin/env python3
"""Tiny opaque Python workload demonstrating all three result modes."""

import argparse
from pathlib import Path

from structured_result import encode_envelope, output_entry, publish_atomic, succeeded_payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("exit-only", "declared-output", "structured"))
    parser.add_argument("workspace", type=Path)
    args = parser.parse_args()

    if args.mode == "exit-only":
        return

    output = args.workspace / "results" / "answer.txt"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("42\n", encoding="utf-8")
    if args.mode == "declared-output":
        return

    payload = succeeded_payload(
        submission_key="python-opaque-example",
        attempt_id="python-opaque-attempt",
        attempt_epoch=1,
        operation={"kind": "script", "sourceDigest": "sha256:" + "c" * 64},
        result_schema="example.answer.v1",
        value=b'{"answer":42}',
        outputs=[output_entry(args.workspace, "results/answer.txt")],
        worker_release={"id": "python-helper-1", "digest": "sha256:" + "d" * 64},
        completed_at="2026-07-22T12:00:00Z",
    )
    publish_atomic(args.workspace / "result.json", encode_envelope(payload))


if __name__ == "__main__":
    main()
