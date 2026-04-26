from types import SimpleNamespace

from lighthouse.detection.asset_classifier import classify_asset_record
from lighthouse.detection.rules import family_holding, sponsorship, vote_holding
from lighthouse.db import queries as q


class _AuditQuery:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        return self.rows


class _AuditSession:
    def __init__(self, rows):
        self.rows = rows

    def query(self, model):
        return _AuditQuery(self.rows)


def test_tickerless_company_name_maps_to_known_sector_and_ticker():
    apple = classify_asset_record({"asset_name": "Apple Inc.", "ticker": None, "asset_type": "unknown", "sector": None})
    microsoft = classify_asset_record({"asset_name": "Microsoft Corporation", "ticker": None, "asset_type": "unknown", "sector": None})
    jpmorgan = classify_asset_record({"asset_name": "JPMorgan Chase & Co.", "ticker": None, "asset_type": "unknown", "sector": None})
    lockheed = classify_asset_record({"asset_name": "Lockheed Martin Corp.", "ticker": None, "asset_type": "unknown", "sector": None})

    assert apple["matched_ticker"] == "AAPL"
    assert apple["sector"] == "information_technology"
    assert apple["classification_confidence"] == "high"
    assert apple["asset_class"] == "public_equity"

    assert microsoft["matched_ticker"] == "MSFT"
    assert microsoft["sector"] == "information_technology"
    assert microsoft["asset_class"] == "public_equity"

    assert jpmorgan["matched_ticker"] == "JPM"
    assert jpmorgan["sector"] == "financials"
    assert jpmorgan["asset_class"] == "public_equity"

    assert lockheed["matched_ticker"] == "LMT"
    assert lockheed["sector"] == "defense"
    assert lockheed["asset_class"] == "public_equity"


def test_diversified_funds_and_fixed_income_assets_classify_conservatively():
    vanguard = classify_asset_record({"asset_name": "Vanguard Total Stock Market Index Fund", "ticker": None, "asset_type": "unknown", "sector": None})
    ishares = classify_asset_record({"asset_name": "iShares S&P 500 ETF", "ticker": None, "asset_type": "unknown", "sector": None})
    muni = classify_asset_record({"asset_name": "City of Austin Municipal Bond", "ticker": None, "asset_type": "unknown", "sector": None})

    assert vanguard["asset_class"] == "diversified_fund"
    assert vanguard["sector"] == "diversified"
    assert vanguard["is_diversified"] is True

    assert ishares["asset_class"] == "diversified_fund"
    assert ishares["sector"] == "diversified"
    assert ishares["classification_confidence"] == "high"

    assert muni["asset_class"] == "municipal_bond"
    assert muni["sector"] == "fixed_income"


def test_real_estate_private_business_and_unknown_assets_stay_conservative():
    rental_property = classify_asset_record({"asset_name": "Rental Property in Austin", "ticker": None, "asset_type": "unknown", "sector": None})
    llc_business = classify_asset_record({"asset_name": "Acme Holdings LLC", "ticker": None, "asset_type": "unknown", "sector": None})
    mystery = classify_asset_record({"asset_name": "Miscellaneous Asset", "ticker": None, "asset_type": "unknown", "sector": None})

    assert rental_property["asset_class"] == "real_estate"
    assert rental_property["sector"] == "real_estate"

    assert llc_business["asset_class"] == "private_business"
    assert llc_business["sector"] == "unknown"

    assert mystery["asset_class"] == "unknown"
    assert mystery["sector"] == "unknown"


def test_municipal_bond_is_skipped_by_vote_holding_detection():
    member_votes = [{"vote_id": "v1", "bill_id": "b1", "position": "Yea"}]
    assets = [
        {
            "id": 1,
            "asset_name": "City of Austin Municipal Bond",
            "ticker": None,
            "value_max": 50000,
            "owner": "self",
            "sector": "fixed_income",
            "asset_class": "municipal_bond",
            "is_diversified": False,
        }
    ]
    bills = {"b1": {"bill_id": "b1", "policy_area": "Financials", "subjects_json": "[]"}}

    assert vote_holding.detect(member_votes, assets, bills) == []


def test_diversified_cash_and_fixed_income_assets_do_not_trigger_low_signal_rules():
    member_votes = [{"vote_id": "v1", "bill_id": "b1", "position": "Yea"}]
    bills = {"b1": {"bill_id": "b1", "policy_area": "Health", "subjects_json": "[]", "govinfo_url": "https://example.test/bill"}}
    low_signal_assets = [
        {
            "id": 1,
            "asset_name": "Vanguard Total Stock Market Index Fund",
            "ticker": None,
            "value_max": 500000,
            "owner": "self",
            "sector": "diversified",
            "asset_class": "diversified_fund",
            "is_diversified": True,
        },
        {
            "id": 2,
            "asset_name": "Bank Sweep Account",
            "ticker": None,
            "value_max": 50000,
            "owner": "self",
            "sector": "cash",
            "asset_class": "cash_or_deposit",
            "is_diversified": False,
        },
        {
            "id": 3,
            "asset_name": "City of Austin Municipal Bond",
            "ticker": None,
            "value_max": 50000,
            "owner": "self",
            "sector": "fixed_income",
            "asset_class": "municipal_bond",
            "is_diversified": False,
        },
    ]

    assert vote_holding.detect(member_votes, low_signal_assets, bills) == []
    assert family_holding.detect(member_votes, low_signal_assets, bills) == []
    assert sponsorship.detect([bills["b1"]], [], low_signal_assets, bills) == []


def test_asset_classification_distribution_counts_unknown_and_classified_assets():
    rows = [
        SimpleNamespace(asset_name="Apple Inc.", ticker=None, asset_type="unknown", sector=None),
        SimpleNamespace(asset_name="City of Austin Municipal Bond", ticker=None, asset_type="unknown", sector=None),
        SimpleNamespace(asset_name="Miscellaneous Asset", ticker=None, asset_type="unknown", sector=None),
    ]

    summary = q.get_asset_classification_distribution(_AuditSession(rows), min_value=0)

    assert summary["sector_counts"]["information_technology"] == 1
    assert summary["asset_class_counts"]["municipal_bond"] == 1
    assert summary["unknown_count"] >= 1
