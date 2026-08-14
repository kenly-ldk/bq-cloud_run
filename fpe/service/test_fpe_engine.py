"""Roundtrip and format-preservation tests for the FF3-1 engine.

Run from the repo root:
    python -m pytest fpe/service/test_fpe_engine.py -q
or standalone:
    python fpe/service/test_fpe_engine.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fpe_engine import Engine, EmailEngine, PASSTHROUGH_COUNTER  # noqa: E402

KEY = "EF4359D8D580AA4F7F036D6F04FC6A94"
TWEAK = "D8E7920AFA330A"

engine = Engine(KEY, TWEAK)
email_engine = EmailEngine(engine)


def test_ssn_roundtrip_and_format():
    for ssn in ["123-45-6789", "987-65-4321", "000-00-0001"]:
        ct = engine.encrypt(ssn, "ssn")
        assert ct != ssn, f"ciphertext equals plaintext for {ssn}"
        assert re.fullmatch(r"\d{3}-\d{2}-\d{4}", ct), f"format broken: {ct}"
        assert engine.decrypt(ct, "ssn") == ssn


def test_email_roundtrip_preserves_domain():
    for addr in [
        "john.smith.42@example.com",
        "emily.brown.1000000@sparkcorners.com",
        "a.b.c.99@test.org",
    ]:
        ct = email_engine.encrypt(addr)
        assert ct != addr
        assert ct.split("@")[1] == addr.split("@")[1], "domain must be preserved"
        assert len(ct) == len(addr), "length must be preserved"
        assert email_engine.decrypt(ct) == addr


def test_name_preserves_spaces_and_length():
    for name in ["John Smith", "Emily Rodriguez"]:
        ct = engine.encrypt(name, "name")
        assert len(ct) == len(name)
        assert [i for i, c in enumerate(ct) if c == " "] == [
            i for i, c in enumerate(name) if c == " "
        ], "space positions must be preserved"
        assert engine.decrypt(ct, "name") == name


def test_short_values_pass_through():
    """Below the FF3-1 domain floor, values are returned unchanged and counted."""
    before = PASSTHROUGH_COUNTER.get("ssn", 0)
    assert engine.encrypt("12", "ssn") == "12"
    assert PASSTHROUGH_COUNTER.get("ssn", 0) == before + 1


def test_deterministic():
    a = engine.encrypt("123-45-6789", "ssn")
    b = engine.encrypt("123-45-6789", "ssn")
    assert a == b, "FPE must be deterministic for search-by-token to work"


def test_unknown_element_raises():
    try:
        engine.encrypt("x" * 10, "not_a_thing")
    except ValueError as exc:
        assert "unknown data element" in str(exc)
    else:
        raise AssertionError("expected ValueError")


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    print(f"\n{'FAILED' if failures else 'ALL PASSED'} ({failures} failure(s))")
    sys.exit(1 if failures else 0)
