from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RISK_KEYWORDS = ("deregistration", "fee_loss", "weak_scoring")
DOCS_VERSION_PATTERN = re.compile(
    r"^\s*Sparket mining interview version:\s*([A-Za-z0-9._-]+)\s*$",
    re.IGNORECASE | re.MULTILINE,
)


def default_docs_path() -> Path:
    return Path(__file__).resolve().parents[2] / "docs" / "miner.md"


def default_attestation_path() -> Path:
    return Path.home() / ".sparket" / "mining_interview.json"


def compute_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def read_docs_hash(path: Path) -> str:
    return compute_sha256(path.read_text(encoding="utf-8"))


def read_interview_docs_version(path: Path) -> str | None:
    text = path.read_text(encoding="utf-8")
    match = DOCS_VERSION_PATTERN.search(text)
    if not match:
        return None
    return match.group(1).strip()


def _normalize_answer(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _load_attestation(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _is_attestation_valid(
    payload: dict[str, Any] | None,
    *,
    docs_hash: str,
    docs_version: str,
) -> bool:
    if not payload:
        return False
    return (
        payload.get("interview_name") == "Sparket mining interview"
        and payload.get("interview_version") == docs_version
        and payload.get("docs_sha256") == docs_hash
        and payload.get("passed") is True
        and isinstance(payload.get("passed_at_utc"), str)
    )


def ensure_interview_passed(
    *,
    docs_path: Path | None = None,
    attestation_path: Path | None = None,
) -> tuple[bool, str]:
    docs = docs_path or default_docs_path()
    attestation = attestation_path or default_attestation_path()

    if not docs.exists():
        return False, f"docs_not_found:{docs}"
    docs_version = read_interview_docs_version(docs)
    if not docs_version:
        return False, "interview_docs_version_missing: add interview version to docs/miner.md"
    docs_hash = read_docs_hash(docs)
    payload = _load_attestation(attestation)
    if _is_attestation_valid(payload, docs_hash=docs_hash, docs_version=docs_version):
        return True, "ok"
    return False, (
        "interview_required: run `python -m sparket.tools.mining_interview` "
        "after reading docs/miner.md"
    )


def _ask_choice(prompt: str, options: list[str], correct_idx: int) -> bool:
    print(f"\n{prompt}")
    for i, option in enumerate(options, start=1):
        print(f"  {i}) {option}")
    raw = input("Answer number: ").strip()
    if not raw.isdigit():
        return False
    idx = int(raw)
    return idx == correct_idx


def _ask_text(prompt: str, expected: str) -> bool:
    print(f"\n{prompt}")
    raw = input("Answer: ")
    return _normalize_answer(raw) == _normalize_answer(expected)


def run_interview(*, docs_path: Path | None = None, attestation_path: Path | None = None) -> int:
    docs = docs_path or default_docs_path()
    attestation = attestation_path or default_attestation_path()

    if not docs.exists():
        print(f"[Sparket mining interview] docs not found: {docs}")
        return 1
    docs_version = read_interview_docs_version(docs)
    if not docs_version:
        print("[Sparket mining interview] missing 'Sparket mining interview version: <value>' in docs/miner.md")
        return 1

    print("=== Sparket mining interview ===")
    print("This gate unlocks the base miner runtime.")
    print("Read docs/miner.md first. One wrong answer fails the interview.\n")

    checks: list[bool] = []
    checks.append(
        _ask_choice(
            "What is the base miner intended for?",
            [
                "A competitive default strategy for rewards",
                "A reference implementation to learn protocol flow",
                "A guaranteed profitable bot",
                "A validator replacement",
            ],
            2,
        )
    )
    checks.append(
        _ask_choice(
            "Running the out-of-the-box base miner unchanged is most likely to:",
            [
                "Outperform the field immediately",
                "Produce random but harmless activity with no downside",
                "Underperform, risk deregistration, and waste registration spend",
                "Bypass validator scoring entirely",
            ],
            3,
        )
    )
    checks.append(
        _ask_choice(
            "If you are not comfortable writing and maintaining custom miner code, you should:",
            [
                "Register anyway and hope defaults win",
                "Not register expecting sustained rewards",
                "Disable logs and continue",
                "Only increase submission frequency",
            ],
            2,
        )
    )
    checks.append(
        _ask_text(
            "Riddle: Enter the risk keywords sorted alphabetically and joined by hyphens.",
            "-".join(sorted(RISK_KEYWORDS)),
        )
    )

    if not all(checks):
        print("\nInterview failed. Re-read docs/miner.md and run again.")
        return 2

    docs_hash = read_docs_hash(docs)
    payload = {
        "interview_name": "Sparket mining interview",
        "interview_version": docs_version,
        "docs_sha256": docs_hash,
        "risk_keywords": sorted(RISK_KEYWORDS),
        "passed": True,
        "passed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    attestation.parent.mkdir(parents=True, exist_ok=True)
    attestation.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nInterview passed. Attestation written to: {attestation}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_interview())

