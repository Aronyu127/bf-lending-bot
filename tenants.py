"""Tenant registry backed by environment variables.

Expected layout (tenant name becomes part of the variable names, so it is
restricted to [A-Za-z0-9_]):

    TENANTS=aron,bob
    TENANT_ARON_BF_API_KEY=...
    TENANT_ARON_BF_API_SECRET=...
    TENANT_ARON_PASSWORD=...
    TENANT_ARON_FUND_CURRENCY=fUSD   # optional
    TENANT_ARON_DRY_RUN=0            # optional

The dashboard resolves the logged-in tenant from the password alone, so
passwords must be unique across tenants.
"""

from __future__ import annotations

import hmac
import os
import re
from dataclasses import dataclass
from typing import List, Optional

DEFAULT_FUND_CURRENCY = "fUSD"

_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
_TRUTHY = ("1", "true", "yes", "on")


class TenantConfigError(ValueError):
    """Raised when the TENANT_* environment layout is unusable."""


@dataclass(frozen=True)
class Tenant:
    name: str
    api_key: str
    api_secret: str
    password: str
    fund_currency: str
    dry_run: bool


def resolve_fund_currency(raw: Optional[str]) -> str:
    if raw is None or not str(raw).strip():
        return DEFAULT_FUND_CURRENCY
    s = str(raw).strip().upper()
    if s.startswith("F") and len(s) > 1:
        return "f" + s[1:]
    return "f" + s


def _env(name: str) -> str:
    return str(os.getenv(name) or "").strip()


def _tenant_names() -> List[str]:
    raw = _env("TENANTS")
    if not raw:
        raise TenantConfigError(
            "TENANTS is unset or empty; expected a comma-separated tenant list (e.g. aron,bob)"
        )
    names = [p.strip() for p in raw.split(",") if p.strip()]
    if not names:
        raise TenantConfigError("TENANTS has no usable entries")
    for n in names:
        if not _NAME_RE.match(n):
            raise TenantConfigError(
                f"tenant name {n!r} must match [A-Za-z0-9_]+ because it forms part of the env var names"
            )
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise TenantConfigError(f"duplicate tenant names in TENANTS: {dupes}")
    return names


def load_tenants() -> List[Tenant]:
    """Read every configured tenant, or raise TenantConfigError."""
    out: List[Tenant] = []
    for name in _tenant_names():
        prefix = f"TENANT_{name.upper()}_"
        key = _env(prefix + "BF_API_KEY")
        secret = _env(prefix + "BF_API_SECRET")
        password = _env(prefix + "PASSWORD")
        missing = [
            prefix + suffix
            for suffix, value in (
                ("BF_API_KEY", key),
                ("BF_API_SECRET", secret),
                ("PASSWORD", password),
            )
            if not value
        ]
        if missing:
            raise TenantConfigError(f"tenant {name!r} is missing: {', '.join(missing)}")
        out.append(
            Tenant(
                name=name,
                api_key=key,
                api_secret=secret,
                password=password,
                fund_currency=resolve_fund_currency(_env(prefix + "FUND_CURRENCY")),
                dry_run=_env(prefix + "DRY_RUN").lower() in _TRUTHY,
            )
        )
    passwords = [t.password for t in out]
    if len(set(passwords)) != len(passwords):
        raise TenantConfigError(
            "two tenants share a dashboard password; login resolves the tenant from the "
            "password alone, so passwords must be unique"
        )
    return out


def find_tenant_by_password(password: str) -> Optional[Tenant]:
    entered = str(password or "").encode("utf-8")
    match: Optional[Tenant] = None
    # Compare against every tenant instead of returning on first hit, so the
    # response time does not reveal which tenant matched.
    for t in load_tenants():
        if hmac.compare_digest(entered, t.password.encode("utf-8")):
            match = t
    return match


def get_tenant(name: str) -> Optional[Tenant]:
    for t in load_tenants():
        if t.name == name:
            return t
    return None
