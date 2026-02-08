# Updated schemas.py
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime


class UnusualSignalOut(BaseModel):
    event_id: int
    ts: datetime
    symbol: str
    member_name: str
    member_bioguide_id: str
    party: Optional[str]
    chamber: str
    trade_type: str
    amount_min: Optional[float]
    amount_max: Optional[float]
    source: str
    baseline_median_amount_max: Optional[float]
    baseline_count: int
    unusual_multiple: float


class UnusualSignalsDebug(BaseModel):
    mode: str
    applied_preset: Optional[str]
    preset_input: Optional[Dict[str, Any]]
    overrides: Dict[str, Any]
    effective_params: Dict[str, Any]

    # NEW explicit fields (safe additive change)
    total_hits: int
    final_hits_count: int
    sort: str
    offset: int


class UnusualSignalsResponse(BaseModel):
    items: List[UnusualSignalOut]
    debug: Optional[UnusualSignalsDebug] = None