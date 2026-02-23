from __future__ import annotations

import json

from sparket.tools.mining_interview import (
    ensure_interview_passed,
    read_docs_hash,
    read_interview_docs_version,
)


def test_ensure_interview_passed_requires_attestation(tmp_path):
    docs = tmp_path / "miner.md"
    docs.write_text(
        "# Miner\nSparket mining interview version: v1\nrisk words here\n",
        encoding="utf-8",
    )
    attestation = tmp_path / "mining_interview.json"

    ok, reason = ensure_interview_passed(docs_path=docs, attestation_path=attestation)
    assert ok is False
    assert "interview_required" in reason


def test_ensure_interview_passed_accepts_valid_attestation(tmp_path):
    docs = tmp_path / "miner.md"
    docs.write_text(
        "# Miner\nSparket mining interview version: v1\ncritical content\n",
        encoding="utf-8",
    )
    docs_hash = read_docs_hash(docs)
    docs_version = read_interview_docs_version(docs)
    assert docs_version == "v1"
    attestation = tmp_path / "mining_interview.json"
    attestation.write_text(
        json.dumps(
            {
                "interview_name": "Sparket mining interview",
                "interview_version": docs_version,
                "docs_sha256": docs_hash,
                "passed": True,
                "passed_at_utc": "2026-02-12T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    ok, reason = ensure_interview_passed(docs_path=docs, attestation_path=attestation)
    assert ok is True
    assert reason == "ok"


def test_ensure_interview_passed_requires_docs_version(tmp_path):
    docs = tmp_path / "miner.md"
    docs.write_text("# Miner\nno version marker\n", encoding="utf-8")
    attestation = tmp_path / "mining_interview.json"
    ok, reason = ensure_interview_passed(docs_path=docs, attestation_path=attestation)
    assert ok is False
    assert "interview_docs_version_missing" in reason

