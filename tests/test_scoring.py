import json

from lighthouse.detection.rules import family_holding, vote_holding
from lighthouse.detection.rules import sponsorship, trade_timing
from lighthouse.detection.rules.vote_holding import ConflictCandidate
from lighthouse.detection.scorer import score_candidates


def _score(candidate: ConflictCandidate) -> dict:
    return score_candidates([candidate])[0]


def test_broad_sector_overlap_does_not_create_high_confidence():
    scored = _score(
        ConflictCandidate(
            conflict_type="vote_holding",
            raw_score=88.0,
            evidence={
                "sector_match": True,
                "source_quality": "public_disclosure_with_bill_and_vote_records",
                "asset_name": "Health Sector Holding",
                "sector": "health_care",
            },
        )
    )

    detail = json.loads(scored["detail_json"])
    assert scored["score"] > 70
    assert scored["confidence"] == "low"
    assert detail["evidence_tier"] == "tier_4_broad_sector"


def test_exact_ticker_match_gets_stronger_tier_than_broad_sector_overlap():
    exact = _score(
        ConflictCandidate(
            conflict_type="vote_holding",
            raw_score=70.0,
            evidence={
                "exact_ticker_match": True,
                "source_quality": "public_disclosure_with_bill_and_vote_records",
            },
        )
    )
    broad = _score(
        ConflictCandidate(
            conflict_type="vote_holding",
            raw_score=70.0,
            evidence={
                "sector_match": True,
                "source_quality": "public_disclosure_with_bill_and_vote_records",
            },
        )
    )

    exact_detail = json.loads(exact["detail_json"])
    broad_detail = json.loads(broad["detail_json"])
    assert exact_detail["evidence_tier"] == "tier_1_exact_company"
    assert broad_detail["evidence_tier"] == "tier_4_broad_sector"


def test_family_holdings_are_discounted_and_limitations_note_indirect_ownership():
    member_votes = [{"vote_id": "v1", "bill_id": "b1", "position": "Yea"}]
    assets = [
        {
            "id": 10,
            "asset_name": "Acme Energy",
            "ticker": "XOM",
            "value_max": 100000,
            "owner": "spouse",
            "sector": "energy",
            "disclosure_id": 1,
            "disclosure_source": "house",
            "disclosure_source_url": "https://example.test/disclosure",
        }
    ]
    bills = {"b1": {"bill_id": "b1", "policy_area": "Energy", "subjects_json": "[]", "govinfo_url": "https://example.test/bill"}}

    candidates = family_holding.detect(member_votes, assets, bills, family_discount=0.6)
    assert candidates

    scored = score_candidates(candidates)[0]
    detail = json.loads(scored["detail_json"])

    assert scored["score"] < 53
    assert "indirect" in " ".join(detail["limitations"]).lower() or "family-held" in " ".join(detail["limitations"]).lower()


def test_diversified_funds_are_skipped():
    member_votes = [{"vote_id": "v1", "bill_id": "b1", "position": "Yea"}]
    assets = [
        {
            "id": 1,
            "asset_name": "Vanguard Total Market Index Fund",
            "ticker": "VTI",
            "value_max": 500000,
            "owner": "self",
            "sector": "diversified",
        }
    ]
    bills = {"b1": {"bill_id": "b1", "policy_area": "Health", "subjects_json": "[]"}}

    assert vote_holding.detect(member_votes, assets, bills) == []


def test_score_and_confidence_are_distinct_concepts():
    scored = _score(
        ConflictCandidate(
            conflict_type="trade_timing_pre",
            raw_score=92.0,
            evidence={
                "sector_match": True,
                "source_quality": "public_disclosure_with_transaction_and_vote_records",
            },
        )
    )
    detail = json.loads(scored["detail_json"])

    assert detail["signal_strength"] == "strong"
    assert scored["confidence"] == "low"


def test_confidence_is_not_derived_from_numeric_score_thresholds():
    broad = _score(
        ConflictCandidate(
            conflict_type="vote_holding",
            raw_score=80.0,
            evidence={
                "sector_match": True,
                "source_quality": "public_disclosure_with_bill_and_vote_records",
            },
        )
    )
    exact = _score(
        ConflictCandidate(
            conflict_type="vote_holding",
            raw_score=80.0,
            evidence={
                "exact_ticker_match": True,
                "source_quality": "public_disclosure_with_bill_and_vote_records",
            },
        )
    )

    assert broad["score"] == exact["score"]
    assert broad["confidence"] == "low"
    assert exact["confidence"] == "high"


def test_evidence_json_contains_required_fields():
    scored = _score(
        ConflictCandidate(
            conflict_type="committee_donor",
            raw_score=40.0,
            evidence={
                "committee_jurisdiction_match": True,
                "source_quality": "public_fec_records_with_committee_metadata",
            },
        )
    )

    detail = json.loads(scored["detail_json"])
    assert "evidence_tier" in detail
    assert "limitations" in detail
    assert "match_reason" in detail


def test_broad_sector_only_rules_do_not_claim_narrow_industry_confidence():
    assets = [
        {
            "id": 1,
            "asset_name": "Broad Energy Holding",
            "ticker": "XOM",
            "value_max": 100000,
            "owner": "self",
            "sector": "energy",
            "disclosure_id": 1,
            "disclosure_source": "house",
            "disclosure_source_url": "https://example.test/disclosure",
        }
    ]
    bill = {
        "bill_id": "b1",
        "title": "An Act concerning energy infrastructure",
        "short_title": None,
        "policy_area": "Energy",
        "subjects_json": "[]",
        "govinfo_url": "https://example.test/bill",
    }

    sponsorship_candidates = sponsorship.detect([bill], [], assets, {"b1": bill})
    sponsorship_detail = json.loads(score_candidates(sponsorship_candidates)[0]["detail_json"])
    assert sponsorship_detail["evidence_tier"] == "tier_4_broad_sector"
    assert sponsorship_detail["confidence"] == "low"

    transactions = [
        {
            "id": 2,
            "ticker": "XOM",
            "asset_name": "Broad Energy Holding",
            "transaction_date": "2024-05-01",
            "transaction_type": "purchase",
            "amount_max": 50000,
            "owner": "self",
            "sector": "energy",
            "source": "house_watcher",
        }
    ]
    votes = [{"vote_id": "v1", "bill_id": "b1", "vote_date": "2024-05-10", "vote_source_url": "https://example.test/vote"}]
    trade_candidates = trade_timing.detect(transactions, votes, {"b1": bill})
    trade_detail = json.loads(score_candidates(trade_candidates)[0]["detail_json"])
    assert trade_detail["evidence_tier"] == "tier_4_broad_sector"
    assert trade_detail["confidence"] == "low"


def test_detail_json_includes_core_provenance_ids():
    scored = _score(
        ConflictCandidate(
            conflict_type="vote_holding",
            raw_score=50.0,
            vote_id="v123",
            bill_id="hr1-119",
            asset_id=7,
            evidence={
                "sector_match": True,
                "source_quality": "public_disclosure_with_bill_and_vote_records",
            },
        )
    )
    detail = json.loads(scored["detail_json"])
    assert detail["vote_id"] == "v123"
    assert detail["bill_id"] == "hr1-119"
    assert detail["asset_id"] == 7
