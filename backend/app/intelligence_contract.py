"""Passive Walnut 2.0 evidence contract; no readers, model calls or route activation.

Callers must authorize and project retained records before constructing a bundle.
Structural grounding does not prove source truth or the meaning of prose. V1
accepts reported numeric facts only; derived calculations need a later contract.
This module avoids replacing the existing app.schemas module with a package.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from hashlib import sha256
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, model_validator

Identifier = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=160)]
Description = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2000)]
Unit = Literal['money', 'money_per_share', 'shares', 'count', 'percent', 'ratio']
Projection = Literal['guest', 'free', 'premium', 'pro', 'admin']
Operation = Literal['analyze_stock', 'find_opportunities', 'get_activity', 'compare_stocks',
                    'get_monitoring_updates', 'get_thesis_changes']


class Contract(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, allow_inf_nan=False)


class Quantity(Contract):
    value: Decimal
    unit: Unit
    currency: Annotated[str, StringConstraints(pattern=r'^[A-Z]{3}$')] | None = None

    @model_validator(mode='after')
    def currency_matches_unit(self):
        if (self.unit in {'money', 'money_per_share'}) != (self.currency is not None):
            raise ValueError('Money requires currency; nonmoney quantities cannot carry currency')
        return self


class EvidenceSource(Contract):
    id: Identifier
    record_id: Identifier
    revision: Identifier
    security_id: int = Field(gt=0)
    locator: Description
    # The facade resolves record_id to its retained URL; clients cannot supply fetch URLs.
    published_at: AwareDatetime | None = None
    event_at: AwareDatetime | None = None
    first_available_at: AwareDatetime


class Observation(Quantity):
    id: Identifier
    source_id: Identifier
    security_id: int = Field(gt=0)
    metric: Identifier
    period_end: date | None = None


class ReportedFact(Quantity):
    id: Identifier
    observation_id: Identifier
    label: Description


class Interpretation(Contract):
    text: Description
    fact_ids: tuple[Identifier, ...] = Field(min_length=1, max_length=20)


class Coverage(Contract):
    state: Literal['complete', 'partial', 'stale', 'unavailable', 'quiet']
    detail: Description


class EvidenceBundle(Contract):
    contract_version: Literal['walnut_intelligence_v1'] = 'walnut_intelligence_v1'
    result_id: Identifier
    operation: Operation
    query: Description
    security_ids: tuple[Annotated[int, Field(gt=0)], ...] = Field(max_length=20)
    data_cutoff: AwareDatetime
    generated_at: AwareDatetime
    last_successful_refresh: AwareDatetime | None
    coverage: Coverage
    access_projection: Projection
    owner_user_id: int | None = Field(default=None, gt=0)
    # Server-computed projection/revision. Neither this field nor owner ID grants access.
    projection_revision: Identifier
    methodology_version: Identifier
    sources: tuple[EvidenceSource, ...] = Field(default=(), max_length=100)
    observations: tuple[Observation, ...] = Field(default=(), max_length=200)
    facts: tuple[ReportedFact, ...] = Field(default=(), max_length=200)
    interpretations: tuple[Interpretation, ...] = Field(default=(), max_length=20)
    risks: tuple[Interpretation, ...] = Field(default=(), max_length=20)
    watch_next: tuple[Interpretation, ...] = Field(default=(), max_length=20)
    limitations: tuple[Description, ...] = Field(default=(), max_length=20)

    @model_validator(mode='after')
    def validate_grounding(self):
        if self.data_cutoff > self.generated_at:
            raise ValueError('Data cutoff follows generation')
        if self.last_successful_refresh and self.last_successful_refresh > self.generated_at:
            raise ValueError('Refresh follows generation')
        if len(set(self.security_ids)) != len(self.security_ids):
            raise ValueError('Duplicate security identity')
        sources = _unique(self.sources)
        observations = _unique(self.observations)
        facts = _unique(self.facts)
        for source in sources.values():
            if source.security_id not in self.security_ids:
                raise ValueError('Source outside security scope')
            if source.first_available_at > self.data_cutoff:
                raise ValueError('Source unavailable at data cutoff')
            if source.published_at and source.published_at > self.data_cutoff:
                raise ValueError('Source publication follows data cutoff')
        for observation in observations.values():
            source = sources.get(observation.source_id)
            if source is None or source.security_id != observation.security_id:
                raise ValueError('Observation source/security mismatch')
        for fact in facts.values():
            observation = observations.get(fact.observation_id)
            if observation is None:
                raise ValueError('Unknown fact observation')
            if (fact.value, fact.unit, fact.currency) != (observation.value, observation.unit, observation.currency):
                raise ValueError('Reported value/unit/currency differs from retained observation')
        for item in (*self.interpretations, *self.risks, *self.watch_next):
            if len(set(item.fact_ids)) != len(item.fact_ids) or not set(item.fact_ids) <= facts.keys():
                raise ValueError('Unknown or duplicate interpretation fact')
        if self.coverage.state == 'complete' and not self.facts:
            raise ValueError('Empty evidence cannot claim complete coverage')
        if self.coverage.state == 'unavailable' and (self.facts or self.observations):
            raise ValueError('Available evidence requires partial/stale coverage')
        if self.operation in {'get_monitoring_updates', 'get_thesis_changes'} and self.owner_user_id is None:
            raise ValueError('Personal operation requires an owner')
        return self


def _unique(items):
    result = {item.id: item for item in items}
    if len(result) != len(items):
        raise ValueError('Duplicate evidence identity')
    return result


def for_reader(bundle: EvidenceBundle, *, authorized_user_id: int | None,
               access_projection: Projection) -> EvidenceBundle:
    """Recheck a cached projection against server-resolved access, not client claims.

    Endpoint authentication, entitlements and record ownership must already have
    been enforced by existing services. This boundary prevents cache mixups only.
    """
    if bundle.access_projection != access_projection:
        raise PermissionError('Evidence projection does not match current access')
    if bundle.owner_user_id is not None and bundle.owner_user_id != authorized_user_id:
        raise PermissionError('Evidence belongs to another account')
    # Revalidate at the consumption boundary, including instances constructed via
    # model_copy/model_construct (which intentionally bypass Pydantic validation).
    return EvidenceBundle.model_validate(bundle.model_dump(mode='python'))


def cache_fingerprint(bundle: EvidenceBundle) -> str:
    """Content fingerprint includes projection, owner and source revisions.

    This is not a cache store, access token or canonical event identity.
    """
    import json
    canonical = json.dumps(bundle.model_dump(mode='json'), sort_keys=True, separators=(',', ':'))
    return sha256(canonical.encode()).hexdigest()
