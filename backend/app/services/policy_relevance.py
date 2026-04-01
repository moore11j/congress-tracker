from __future__ import annotations

from dataclasses import dataclass

from app.models import Member

PolicyDomain = str


@dataclass(frozen=True)
class PolicyRelevanceResult:
    committee_relevant: bool
    relevance_domain: PolicyDomain | None
    relevance_label: str | None
    member_domains: tuple[PolicyDomain, ...]
    ticker_domains: tuple[PolicyDomain, ...]

    def as_dict(self) -> dict:
        return {
            "committee_relevant": self.committee_relevant,
            "relevance_domain": self.relevance_domain,
            "relevance_label": self.relevance_label,
            "member_policy_domains": list(self.member_domains),
            "ticker_policy_domains": list(self.ticker_domains),
        }


# Conservative, explicit Phase 1 mapping. We only tag members when we have
# a clear policy-domain signal from a curated list.
_MEMBER_DOMAIN_OVERRIDES: dict[str, tuple[PolicyDomain, ...]] = {
    "P000197": ("technology",),  # Nancy Pelosi
    "C001073": ("banking",),  # Tommy Tuberville (banking committee)
    "G000386": ("banking",),  # Josh Gottheimer (financial services)
    "M001153": ("banking",),  # Roger Marshall (banking committee)
    "R000307": ("defense",),  # Mike Rogers (armed services)
    "W000802": ("defense",),  # Roger Wicker (armed services)
    "S001141": ("healthcare",),  # Bill Cassidy (health/education/labor)
    "M001111": ("healthcare",),  # Patty Murray (health/education/labor)
    "B001288": ("agriculture",),  # John Boozman (agriculture)
    "S001203": ("agriculture",),  # Debbie Stabenow (agriculture)
    "C001047": ("energy",),  # Shelley Moore Capito (environment/public works)
    "H001075": ("energy",),  # Martin Heinrich (energy and natural resources)
    "C001056": ("transportation",),  # Maria Cantwell (commerce/science/transportation)
}

_DOMAIN_LABELS: dict[PolicyDomain, str] = {
    "defense": "Defense",
    "healthcare": "Healthcare",
    "banking": "Banking & financial policy",
    "energy": "Energy",
    "technology": "Technology",
    "agriculture": "Agriculture",
    "transportation": "Transportation",
}


def _normalize(value: str | None) -> str:
    return (value or "").strip().upper()


def domains_for_member(member: Member | None) -> tuple[PolicyDomain, ...]:
    if not member:
        return ()
    key = _normalize(member.bioguide_id)
    return _MEMBER_DOMAIN_OVERRIDES.get(key, ())


def domains_for_ticker(*, symbol: str | None, sector: str | None, security_name: str | None) -> tuple[PolicyDomain, ...]:
    sector_l = (sector or "").strip().lower()
    name_l = (security_name or "").strip().lower()
    symbol_u = _normalize(symbol)

    domains: list[PolicyDomain] = []

    if any(token in sector_l for token in ("technology", "software", "semiconductor", "internet", "communication")):
        domains.append("technology")
    if any(token in sector_l for token in ("financial", "bank", "capital markets", "insurance")):
        domains.append("banking")
    if any(token in sector_l for token in ("health", "biotech", "pharma", "medical")):
        domains.append("healthcare")
    if any(token in sector_l for token in ("energy", "oil", "gas", "utilities")):
        domains.append("energy")
    if any(token in sector_l for token in ("aerospace", "defense")):
        domains.append("defense")
    if any(token in sector_l for token in ("transport", "airline", "rail", "logistics")):
        domains.append("transportation")
    if any(token in sector_l for token in ("agriculture", "farm")):
        domains.append("agriculture")

    if symbol_u in {"LMT", "NOC", "RTX", "GD"} or "defense" in name_l or "aerospace" in name_l:
        domains.append("defense")

    deduped = tuple(sorted(set(domains)))
    return deduped


def resolve_policy_relevance(
    *,
    member: Member | None,
    symbol: str | None,
    sector: str | None,
    security_name: str | None,
) -> PolicyRelevanceResult:
    member_domains = domains_for_member(member)
    ticker_domains = domains_for_ticker(symbol=symbol, sector=sector, security_name=security_name)

    overlap = sorted(set(member_domains).intersection(ticker_domains))
    if overlap:
        domain = overlap[0]
        label = f"Policy-domain relevant: {_DOMAIN_LABELS.get(domain, domain.title())}"
        return PolicyRelevanceResult(
            committee_relevant=True,
            relevance_domain=domain,
            relevance_label=label,
            member_domains=member_domains,
            ticker_domains=ticker_domains,
        )

    return PolicyRelevanceResult(
        committee_relevant=False,
        relevance_domain=None,
        relevance_label=None,
        member_domains=member_domains,
        ticker_domains=ticker_domains,
    )
