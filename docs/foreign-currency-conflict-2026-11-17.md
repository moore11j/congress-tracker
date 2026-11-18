# Conflicting reporting currencies in one SEC original

The [JCSE June 2026 interim filing](https://www.sec.gov/Archives/edgar/data/1905511/000149315226047701/form6-k.htm) contains a USD reporting-currency statement in its risk discussion. Note 2(e) identifies Singaporean dollars as the statement reporting currency and describes the USD column as a convenience translation. Both declarations must be considered before selecting a currency.

The shared declaration reader now recognizes accompanying statements, an article before the currency, and the Singaporean spelling. Contradictory declarations remain ambiguous. It does not choose SGD, infer exchange rates or combine convenience translations across periods. The original's immutable digest is also rejected when validating an older cached declaration that missed the contradiction. Other originals and historical evidence keep their existing validation.

The complete three-source reader fixture preserves verified issuer identity while withholding financial amounts and periods. Repeated acquisition uses identical requests and produces identical identity-only values. Tests cover the full original, old cached evidence, supported wording and misleading subsidiary, historical, future and negated statements. This change does not establish complete financial coverage.
