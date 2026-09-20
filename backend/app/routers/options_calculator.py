from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from app.rate_limit import rate_limit_provider_backed
from app.services.options_calculator import OptionsDataError, contracts, expirations, previous_close
from app.services.options_alpaca import enabled as alpaca_enabled, prices as alpaca_prices

router = APIRouter(prefix="/tools/options", tags=["options-calculator"], dependencies=[Depends(rate_limit_provider_backed)])


def _data(action, *args):
    try:
        return action(*args)
    except OptionsDataError as error:
        raise HTTPException(error.status, str(error), headers={"Retry-After": "61"} if error.status == 429 else None) from None


@router.get("/contracts")
def option_contracts(symbol: str = Query(pattern=r"^[A-Z][A-Z0-9.\-]{0,9}$"), expiration: date = Query(), cursor: str | None = Query(None, max_length=4096, pattern=r"^[A-Za-z0-9_+=/\-]+$")):
    return _data(contracts, symbol, expiration.isoformat(), cursor)


@router.get("/expirations")
def option_expirations(symbol: str = Query(pattern=r"^[A-Z][A-Z0-9.\-]{0,9}$"), spot: float = Query(gt=0, le=1000000), cursor: str | None = Query(None, max_length=4096, pattern=r"^[A-Za-z0-9_+=/\-]+$")):
    return _data(expirations, symbol, spot, cursor)


@router.get("/close")
def option_close(ticker: str = Query(pattern=r"^(?:[A-Z][A-Z0-9.\-]{0,9}|O:[A-Z0-9.\-]{1,10}\d{6}[CP]\d{8})$")):
    return _data(previous_close, ticker)


@router.get("/prices")
def option_prices(tickers: str = Query(min_length=1, max_length=3300)):
    if not alpaca_enabled():
        raise HTTPException(503, "Batch historical prices are not enabled. Use individual closes or manual premiums.")
    return _data(alpaca_prices, tickers.split(","))
