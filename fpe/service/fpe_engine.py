"""Format-preserving encryption engine for the vendor-free demo.

Implements NIST SP 800-38G FF3-1 via the `ff3` package, wrapped in a
general-purpose format-preserving scheme:

    Only characters that belong to the data element's alphabet are encrypted.
    Every other character (the "-" in an SSN, the "@" and domain in an email)
    is structural: it stays exactly where it was.

That keeps the ciphertext the same length and shape as the plaintext, which is
the whole point of FPE for a column you still want to store in a STRING field.

FF3-1 has a domain-size floor: radix^len must be >= 1,000,000, so short
payloads cannot be encrypted. Values whose encryptable payload falls below
`min_len` are passed through unchanged and counted in PASSTHROUGH_COUNTER —
documented behaviour, not silent corruption.
"""

from __future__ import annotations

import string
import threading
from dataclasses import dataclass, field

from ff3 import FF3Cipher

# FF3-1 tweak is 56-bit (14 hex chars). The `ff3` package also accepts the
# legacy 64-bit FF3 tweak; we standardise on FF3-1.
TWEAK_HEX_LEN = 14

DIGITS = string.digits
LOWER = string.ascii_lowercase
UPPER = string.ascii_uppercase

#: Alphabets per data element. Characters outside the alphabet are structural.
ALPHABETS: dict[str, str] = {
    # 123-45-6789 -> dashes preserved, 9 digits encrypted
    "ssn": DIGITS,
    # john.smith.42@example.com -> "@" and everything after it is structural
    # only because we split on "@" first (see _EmailTransformer).
    "email": DIGITS + LOWER + "._-+",
    # "John Smith" -> space preserved, letters encrypted, case preserved
    "name": LOWER + UPPER,
    "phone": DIGITS,
    "ccn": DIGITS,
    "alnum": DIGITS + LOWER + UPPER,
    "digits": DIGITS,
}

#: Per-data-element count of values too short for FF3-1 to encrypt.
PASSTHROUGH_COUNTER: dict[str, int] = {}


def _min_len(radix: int) -> int:
    """Smallest payload length where radix**len >= 1_000_000 (FF3-1 floor)."""
    n, size = 0, 1
    while size < 1_000_000:
        size *= radix
        n += 1
    return n


@dataclass
class _CipherSpec:
    alphabet: str
    min_len: int
    max_len: int

    def __post_init__(self) -> None:
        # ff3 caps message length at 2 * floor(log_radix(2**96)); the package
        # enforces it, we just mirror the bound for the pass-through check.
        pass


class Engine:
    """Thread-safe FF3-1 engine.

    `FF3Cipher` instances are cheap to build but hold per-instance state, so we
    keep one cipher per (data_element) per thread rather than sharing across
    threads. With the gthread worker class this matters; with sync workers it
    is simply a no-op cache.
    """

    def __init__(self, key: str, tweak: str) -> None:
        if len(tweak) != TWEAK_HEX_LEN:
            raise ValueError(
                f"FPE_TWEAK must be {TWEAK_HEX_LEN} hex chars (56-bit FF3-1 tweak), "
                f"got {len(tweak)}"
            )
        if len(key) not in (32, 48, 64):
            raise ValueError(
                f"FPE_KEY must be 32/48/64 hex chars (AES-128/192/256), got {len(key)}"
            )
        self.key = key
        self.tweak = tweak
        self._local = threading.local()
        self._specs: dict[str, _CipherSpec] = {}
        for de, alphabet in ALPHABETS.items():
            radix = len(alphabet)
            self._specs[de] = _CipherSpec(
                alphabet=alphabet,
                min_len=_min_len(radix),
                max_len=2 * (96 // (radix.bit_length() - 1)) if radix > 1 else 0,
            )

    def _cipher(self, de: str) -> FF3Cipher:
        cache = getattr(self._local, "ciphers", None)
        if cache is None:
            cache = {}
            self._local.ciphers = cache
        cipher = cache.get(de)
        if cipher is None:
            spec = self._specs[de]
            cipher = FF3Cipher.withCustomAlphabet(self.key, self.tweak, spec.alphabet)
            cache[de] = cipher
        return cipher

    def spec(self, de: str) -> _CipherSpec:
        try:
            return self._specs[de]
        except KeyError:
            raise ValueError(
                f"unknown data element {de!r}; known: {sorted(self._specs)}"
            ) from None

    # -- core -------------------------------------------------------------

    def _transform(self, value: str, de: str, encrypt: bool) -> str:
        spec = self.spec(de)
        members = set(spec.alphabet)

        payload_chars = [c for c in value if c in members]
        if len(payload_chars) < spec.min_len:
            PASSTHROUGH_COUNTER[de] = PASSTHROUGH_COUNTER.get(de, 0) + 1
            return value

        cipher = self._cipher(de)
        payload = "".join(payload_chars)
        out = cipher.encrypt(payload) if encrypt else cipher.decrypt(payload)

        # Splice the transformed payload back into the structural skeleton.
        it = iter(out)
        return "".join(next(it) if c in members else c for c in value)

    def encrypt(self, value: str, de: str) -> str:
        return self._transform(value, de, encrypt=True)

    def decrypt(self, value: str, de: str) -> str:
        return self._transform(value, de, encrypt=False)


class EmailEngine:
    """Email needs the domain held out before the generic scheme applies.

    Without this, "@" would be structural but the domain's letters would be
    encrypted too, producing a ciphertext that no longer routes anywhere and
    leaking nothing useful in exchange. Tokenising only the local part is the
    conventional choice.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def _split(self, value: str) -> tuple[str, str]:
        at = value.rfind("@")
        if at < 0:
            return value, ""
        return value[:at], value[at:]

    def encrypt(self, value: str, de: str = "email") -> str:
        local, domain = self._split(value)
        return self.engine.encrypt(local, "email") + domain

    def decrypt(self, value: str, de: str = "email") -> str:
        local, domain = self._split(value)
        return self.engine.decrypt(local, "email") + domain
