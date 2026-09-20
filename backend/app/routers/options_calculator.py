from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from app.rate_limit import rate_limit_provider_backed
from app.services.options_calculator import OptionsDataError, contracts, previous_close

router = APIRouter(prefix="/tools/options", tags=["options-calculator"], dependencies=[Depends(rate_limit_provider_backed)])


def _data(action, *args):
    try:
        return action(*args)
    except OptionsDataError as error:
        raise HTTPException(error.status, str(error), headers={"Retry-After": "61"} if error.status == 429 else None) from None


@router.get("/contracts")
def option_contracts(symbol: str = Query(pattern=r"^[A-Z][A-Z0-9.\-]{0,9}$"), expiration: date = Query()):
    return _data(contracts, symbol, expiration.isoformat())


@router.get("/close")
def option_close(ticker: str = Query(pattern=r"^(?:[A-Z][A-Z0-9.\-]{0,9}|O:[A-Z0-9.\-]{1,10}\d{6}[CP]\d{8})$")):
    return _data(previous_close, ticker)
