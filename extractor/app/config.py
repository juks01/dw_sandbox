from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

CONFIG_PATH = Path(__file__).resolve().parent.parent / "conf" / "allowed_hosts"


def _read_allowed_hosts_from_file() -> set[str]:
    if not CONFIG_PATH.exists():
        return set()

    hosts: set[str] = set()
    for line in CONFIG_PATH.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if value and not value.startswith("#"):
            hosts.add(value)
    return hosts


def is_allowed_url(raw_url: str) -> bool:
    value = raw_url.strip()
    if not value:
        return False

    allowed_hosts = _read_allowed_hosts_from_file()
    normalized_allowed = {host.strip().lower() for host in allowed_hosts}
    if value in normalized_allowed:
        return True

    try:
        parsed = urlparse(value)
    except ValueError:
        return False

    host = (parsed.hostname or "").lower()
    if not host:
        return False

    for host in allowed_hosts:
        candidate = host.strip().lower()
        if candidate.startswith("*."):
            suffix = candidate[2:]
            if host == suffix or host.endswith(f".{suffix}"):
                return True
        if host == candidate:
            return True

    return False
