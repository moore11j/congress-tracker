"""Print a Stripe catalog plan; --apply creates sandbox objects only. Never live."""
import argparse
import json
import os

from app.services.research_credit_checkout import SandboxStripe, CATALOG_VERSION, CreditError
from app.services.research_credits import PURCHASE_CENTS_PER_CREDIT, MONTHLY_ALLOWANCE

CATALOG = [
    {"key": "premium", "name": "Walnut Premium", "credits": MONTHLY_ALLOWANCE["premium"],
     "prices": [("monthly", 2495, "month"), ("annual", 24950, "year")]},
    {"key": "pro", "name": "Walnut Pro", "credits": MONTHLY_ALLOWANCE["pro"],
     "prices": [("monthly", 3995, "month"), ("annual", 39995, "year")]},
    {"key": "credits", "name": "Walnut Research Credits", "credits": None,
     "prices": [("unit", PURCHASE_CENTS_PER_CREDIT, None)]},
]


def install_catalog(stripe):
    receipt = {"sandbox": True, "catalog": CATALOG_VERSION, "prices": {}}
    for product in CATALOG:
        product_id = f"{CATALOG_VERSION}_{product['key']}"
        # Stable product IDs prevent duplicate creation on retries beyond idempotency TTL.
        listed = stripe.request("GET", "products", {"ids[]": product_id, "limit": 1}).get("data", [])
        if listed:
            existing = listed[0]
            if (existing.get("livemode") is not False or existing.get("id") != product_id
                    or existing.get("active") is not True or existing.get("name") != product["name"]
                    or existing.get("metadata", {}).get("monthly_research_credits") != (str(product["credits"]) if product["credits"] else None)
                    or existing.get("metadata", {}).get("walnut_catalog") != CATALOG_VERSION):
                raise CreditError("Existing product conflicts with the sandbox catalog")
        else:
            fields = {"id": product_id, "name": product["name"], "metadata[walnut_catalog]": CATALOG_VERSION}
            if product["credits"]:
                fields["metadata[monthly_research_credits]"] = product["credits"]
            stripe.request("POST", "products", fields, idempotency=product_id)
        for interval, cents, recurring in product["prices"]:
            lookup = f"{product_id}_{interval}"
            existing = stripe.request("GET", "prices", {"lookup_keys[]": lookup, "limit": 2}).get("data", [])
            if existing:
                if len(existing) != 1:
                    raise CreditError("Duplicate price lookup")
                price = existing[0]
                if (price.get("livemode") is not False or price.get("active") is not True
                        or price.get("product") != product_id or price.get("currency") != "usd"
                        or price.get("unit_amount") != cents
                        or price.get("billing_scheme") != "per_unit" or price.get("transform_quantity")
                        or (price.get("recurring") or {}).get("interval") != recurring
                        or (recurring and (price.get("recurring") or {}).get("interval_count") != 1)
                        or price.get("metadata", {}).get("walnut_catalog") != CATALOG_VERSION):
                    raise CreditError("Existing price conflicts with sandbox plan")
            else:
                fields = {"product": product_id, "currency": "usd", "unit_amount": cents,
                          "lookup_key": lookup, "metadata[walnut_catalog]": CATALOG_VERSION}
                if recurring:
                    fields["recurring[interval]"] = recurring
                price = stripe.request("POST", "prices", fields, idempotency=lookup)
            receipt["prices"][f"{product['key']}_{interval}"] = price["id"]
    return receipt


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Create only the isolated Stripe sandbox catalog")
    args = parser.parse_args()
    if not args.apply:
        print(json.dumps({"action": "plan_only", "catalog": CATALOG, "live_mutations": False}, indent=2))
    else:
        # Deliberately no fallback to STRIPE_SECRET_KEY.
        stripe = SandboxStripe(os.getenv("WALNUT_STRIPE_SANDBOX_SECRET_KEY", ""))
        print(json.dumps(install_catalog(stripe), indent=2))
