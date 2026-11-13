"""Filter a pg_restore TOC listing (``pg_restore -l``) down to plain tables
plus their indexes, constraints and backing sequences/defaults.

Everything else (extensions, views, functions, comments, ACLs, ...) is
commented out so that ``pg_restore -L`` skips it, and counted so the user can
see which unsupported object kinds were excluded.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

_ENTRY_RE = re.compile(r"^(?P<dumpid>\d+);\s+(?P<cat1>\d+)\s+(?P<cat2>\d+)\s+(?P<rest>.*)$")

# Pseudo entries that must stay active for ``pg_restore -L`` to work correctly.
_KEEP_ALWAYS = ("ENCODING", "STDSTRINGS")

# Object kinds we restore: plain tables + data, indexes, constraints, and the
# sequences/defaults backing identity/serial columns.
_ALLOWED = (
    "TABLE DATA",
    "TABLE",
    "INDEX",
    "CONSTRAINT",
    "FK CONSTRAINT",
    "SEQUENCE SET",
    "SEQUENCE OWNED BY",
    "SEQUENCE",
    "DEFAULT",
    "SCHEMA",
)

# Known TOC entry types, longest first so prefix matching is unambiguous.
_KNOWN_TYPES = sorted(
    set(_ALLOWED)
    | {
        "MATERIALIZED VIEW DATA",
        "MATERIALIZED VIEW",
        "DEFAULT ACL",
        "EVENT TRIGGER",
        "FOREIGN DATA WRAPPER",
        "FOREIGN SERVER",
        "FOREIGN TABLE",
        "USER MAPPING",
        "OPERATOR CLASS",
        "OPERATOR",
        "ROW SECURITY",
        "ACCESS METHOD",
        "PUBLICATION",
        "SUBSCRIPTION",
        "STATISTICS",
        "EXTENSION",
        "COMMENT",
        "FUNCTION",
        "PROCEDURE",
        "AGGREGATE",
        "TYPE",
        "DOMAIN",
        "VIEW",
        "TRIGGER",
        "RULE",
        "ACL",
        "BLOB",
        "BLOBS",
        "CAST",
        "SERVER",
        "COLLATION",
        "CONVERSION",
        "LANGUAGE",
        "TRANSFORM",
        "POLICY",
        "ENCODING",
        "STDSTRINGS",
    },
    key=len,
    reverse=True,
)


@dataclass
class TocFilterResult:
    lines: list[str]
    kept: int = 0
    skipped: dict[str, int] = field(default_factory=dict)


def _split_type(rest: str) -> tuple[str, str]:
    """Split 'TABLE DATA public customers postgres' -> ('TABLE DATA', 'public customers postgres')."""
    for t in _KNOWN_TYPES:
        if rest == t or rest.startswith(t + " "):
            return t, rest[len(t):].strip()
    # Unknown type: treat the first token as the type label (it will be skipped).
    parts = rest.split(None, 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def filter_toc(toc_text: str) -> TocFilterResult:
    out: list[str] = []
    res = TocFilterResult(lines=out)
    for line in toc_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(";"):
            out.append(line)
            continue
        m = _ENTRY_RE.match(line)
        if not m:
            out.append(line)
            continue
        rest = m.group("rest")
        otype, remainder = _split_type(rest)
        if otype in _KEEP_ALWAYS:
            out.append(line)
            continue
        if otype == "SCHEMA":
            # 'SCHEMA - public postgres': fields are <schema> <name> <owner>,
            # and the schema field is '-' here. A fresh database already has
            # the public schema; recreating it would error out.
            tokens = remainder.split()
            name = tokens[-2] if len(tokens) >= 2 else (tokens[0] if tokens else "")
            if name == "public":
                out.append("; " + line + "   ; skipped: public schema already exists")
                continue
            out.append(line)
            res.kept += 1
            continue
        if otype in _ALLOWED:
            out.append(line)
            res.kept += 1
        else:
            out.append("; " + line + f"   ; skipped: unsupported object type {otype}")
            res.skipped[otype] = res.skipped.get(otype, 0) + 1
    return res
