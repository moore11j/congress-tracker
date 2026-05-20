from app.services.congress_assets import (
    CANONICAL_CRYPTO_BUCKET,
    CANONICAL_ETF_FUND_BUCKET,
    CANONICAL_OTHER_BUCKET,
    CANONICAL_PUBLIC_EQUITY_BUCKET,
    CANONICAL_TREASURY_BUCKET,
    canonical_asset_bucket,
    classify_congress_disclosure_asset,
)


def test_treasury_etf_classified_as_etf_fund_not_treasury():
    assert (
        canonical_asset_bucket(
            event_type="congress_trade",
            asset_class="ETF",
            symbol="IEI",
            security_description="iShares 3-7 Year Treasury Bond ETF",
            company_name="iShares 3-7 Year Treasury Bond ETF",
        )
        == CANONICAL_ETF_FUND_BUCKET
    )
    assert classify_congress_disclosure_asset(
        security_description="iShares 3-7 Year Treasury Bond ETF",
        asset_class="ETF",
        raw_symbol="IEI",
    ) is None


def test_direct_us_treasury_bills_classified_as_treasury():
    classification = classify_congress_disclosure_asset(
        security_description="13 Week U.S. Treasury Bills",
        asset_class="Government Security",
    )
    assert classification is not None
    assert classification.asset_class == "treasury"
    assert classification.event_type == "congress_treasury_trade"
    assert (
        canonical_asset_bucket(
            event_type=classification.event_type,
            asset_class=classification.asset_class,
            instrument_type=classification.instrument_type,
            security_description=classification.security_description,
        )
        == CANONICAL_TREASURY_BUCKET
    )


def test_direct_crypto_classified_as_crypto_but_crypto_etf_is_fund():
    classification = classify_congress_disclosure_asset(
        security_description="Bitcoin",
        asset_class="Cryptocurrency",
        raw_symbol="BTC",
    )
    assert classification is not None
    assert classification.asset_class == "crypto"
    assert classification.event_type == "congress_crypto_trade"

    assert (
        canonical_asset_bucket(
            event_type="congress_trade",
            asset_class="ETF",
            symbol="IBIT",
            security_description="iShares Bitcoin Trust ETF",
        )
        == CANONICAL_ETF_FUND_BUCKET
    )
    assert classify_congress_disclosure_asset(
        security_description="iShares Bitcoin Trust ETF",
        asset_class="ETF",
        raw_symbol="IBIT",
    ) is None


def test_symbol_linked_public_security_never_classifies_as_other():
    assert (
        canonical_asset_bucket(
            event_type="congress_trade",
            asset_class=None,
            symbol="JPM",
            security_description="JPMorgan Chase & Co",
        )
        == CANONICAL_PUBLIC_EQUITY_BUCKET
    )
    assert (
        canonical_asset_bucket(
            event_type="congress_trade",
            symbol=None,
            security_description="Unresolved security",
        )
        == CANONICAL_OTHER_BUCKET
    )
