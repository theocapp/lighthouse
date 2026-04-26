"""
Builds a structured report dict for a single member from DB data.
Passed to formatters for output as JSON, CSV, or HTML.
"""
import json
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from ..db import queries as q
from ..db.models import Member


def build_report(session: Session, bioguide_id: str) -> Optional[dict]:
    member = session.get(Member, bioguide_id)
    if not member:
        return None

    conflicts = q.get_conflicts_for_member(session, bioguide_id)
    assets = q.get_member_assets(session, bioguide_id, min_value=0)
    transactions = q.get_member_transactions(session, bioguide_id)
    committees = q.get_committee_memberships(session, bioguide_id, congress=0)
    identifiers = q.get_member_identifiers(session, bioguide_id)
    contributions = q.get_contributions(session, bioguide_id)
    sponsored_bills = q.get_sponsored_bills(session, bioguide_id)
    cosponsor_rows = q.get_cosponsored_bills(session, bioguide_id)
    vote_stats = q.get_member_vote_stats(session, bioguide_id)
    recent_votes = q.get_member_recent_votes(session, bioguide_id, limit=25)
    data_coverage = q.get_data_coverage(session)

    high = [c for c in conflicts if c["confidence"] == "high"]
    medium = [c for c in conflicts if c["confidence"] == "medium"]
    low = [c for c in conflicts if c["confidence"] == "low"]
    max_score = max((c["score"] for c in conflicts), default=0.0)

    by_type: dict[str, list[dict]] = {}
    for c in conflicts:
        by_type.setdefault(c["conflict_type"], []).append(c)

    # Portfolio analytics
    sector_totals: dict[str, float] = {}
    type_totals: dict[str, float] = {}
    total_value_min = 0.0
    total_value_max = 0.0
    for a in assets:
        val_max = a.get("value_max") or 0
        val_min = a.get("value_min") or 0
        sector = a.get("sector") or "other"
        atype = a.get("asset_type") or "other"
        sector_totals[sector] = sector_totals.get(sector, 0) + val_max
        type_totals[atype] = type_totals.get(atype, 0) + val_max
        total_value_min += val_min
        total_value_max += val_max

    top_holdings = sorted(assets, key=lambda a: a.get("value_max") or 0, reverse=True)[:12]
    all_assets_sorted = sorted(assets, key=lambda a: a.get("value_max") or 0, reverse=True)

    # Trade analytics
    buys = [t for t in transactions if t.get("transaction_type") == "purchase"]
    sells = [t for t in transactions if "sale" in (t.get("transaction_type") or "")]
    trade_tickers: dict[str, int] = {}
    for t in transactions:
        tk = t.get("ticker") or "—"
        trade_tickers[tk] = trade_tickers.get(tk, 0) + 1
    top_traded = sorted(trade_tickers.items(), key=lambda x: x[1], reverse=True)[:8]
    latest_trade_date = max((t.get("transaction_date") or "" for t in transactions), default=None)
    latest_vote_date = max((v.get("vote_date") or "" for v in recent_votes), default=None)
    funding_summary = _build_funding_summary(contributions)

    # Committee overlap with holdings (sectors that appear in both)
    committee_sectors: set[str] = set()
    for cm in committees:
        name = (cm.get("committee_name") or "").lower()
        if any(k in name for k in ("energy", "natural")):
            committee_sectors.add("energy")
        if any(k in name for k in ("financial", "bank", "finance")):
            committee_sectors.add("financials")
        if any(k in name for k in ("health", "aging", "medical")):
            committee_sectors.add("health_care")
        if any(k in name for k in ("tech", "commerce", "science", "innov")):
            committee_sectors.add("information_technology")
        if any(k in name for k in ("defense", "armed", "military")):
            committee_sectors.add("industrials")
    holding_sectors = set(sector_totals.keys()) - {"other", "diversified", "unknown"}
    overlap_sectors = list(committee_sectors & holding_sectors)

    return {
        "generated_at": datetime.utcnow().isoformat(),
        "member": {
            "bioguide_id": member.bioguide_id,
            "full_name": member.full_name,
            "first_name": member.first_name,
            "last_name": member.last_name,
            "party": member.party,
            "state": member.state,
            "district": member.district,
            "chamber": member.chamber,
            "fec_candidate_id": member.fec_candidate_id,
            "initials": _initials(member.full_name),
            "display_title": "Senator" if member.chamber == "senate" else "Representative",
            "seat_label": _seat_label(member.chamber, member.state, member.district),
            "profile_image_url": _profile_image_url(member.bioguide_id),
            "profile_image_fallback_url": _legacy_profile_image_url(member.bioguide_id),
        },
        "identity": {
            "identifiers": identifiers,
            "grouped_identifiers": _group_identifiers(identifiers),
        },
        "summary": {
            "total_conflicts": len(conflicts),
            "total_signals": len(conflicts),
            "high_confidence": len(high),
            "medium_confidence": len(medium),
            "low_confidence": len(low),
            "max_score": round(max_score, 2),
            "total_assets": len(assets),
            "total_transactions": len(transactions),
            "committee_count": len(committees),
            "total_contributions": funding_summary["count"],
            "total_contribution_amount": funding_summary["total_amount"],
            "committees": [c["committee_name"] for c in committees],
            "committee_roles": committees,
            "disclaimer": (
                "Lighthouse does not prove corruption, illegality, intent, or misconduct. "
                "It identifies evidence-backed signals that may deserve review."
            ),
        },
        "coverage": {
            "has_conflicts": bool(conflicts),
            "has_signals": bool(conflicts),
            "has_assets": bool(assets),
            "has_transactions": bool(transactions),
            "has_committees": bool(committees),
            "has_votes": bool(vote_stats.get("total")),
            "has_sponsored_bills": bool(sponsored_bills),
            "has_funding": bool(contributions),
            "latest_trade_date": latest_trade_date,
            "latest_vote_date": latest_vote_date,
            "latest_contribution_date": funding_summary["latest_date"],
            "data_coverage": data_coverage,
        },
        "portfolio": {
            "total_value_min": int(total_value_min),
            "total_value_max": int(total_value_max),
            "by_sector": dict(sorted(sector_totals.items(), key=lambda x: x[1], reverse=True)),
            "by_type": dict(sorted(type_totals.items(), key=lambda x: x[1], reverse=True)),
            "top_holdings": top_holdings,
            "overlap_sectors": overlap_sectors,
        },
        "trading": {
            "buy_count": len(buys),
            "sell_count": len(sells),
            "top_tickers": top_traded,
        },
        "funding": funding_summary,
        "voting": {
            "stats": vote_stats,
            "recent": recent_votes,
        },
        "legislation": {
            "sponsored": sponsored_bills[:20],
            "sponsored_count": len(sponsored_bills),
            "cosponsored_count": len(cosponsor_rows),
        },
        "disclaimer": (
            "Lighthouse does not prove corruption, illegality, intent, or misconduct. "
            "It surfaces public-data signals that may deserve review."
        ),
        "conflicts": conflicts,
        "conflicts_by_type": by_type,
        "assets": assets,
        "all_assets_sorted": all_assets_sorted,
        "contributions": contributions,
        "recent_transactions": sorted(
            transactions,
            key=lambda t: t.get("transaction_date") or "",
            reverse=True,
        )[:50],
    }


def _initials(full_name: str) -> str:
    parts = full_name.split()
    if len(parts) >= 2:
        return f"{parts[0][0]}{parts[-1][0]}".upper()
    return full_name[:2].upper() if full_name else "?"


def _profile_image_url(bioguide_id: str) -> str:
    initial = (bioguide_id or "X")[:1].upper()
    return f"https://bioguide.congress.gov/bioguide/photo/{initial}/{bioguide_id.upper()}.jpg"


def _legacy_profile_image_url(bioguide_id: str) -> str:
    initial = (bioguide_id or "X")[:1].upper()
    return f"http://bioguide.congress.gov/bioguide/photo/{initial}/{bioguide_id.upper()}.jpg"


def _seat_label(chamber: str, state: Optional[str], district: Optional[int]) -> str:
    if chamber == "senate":
        return f"{state} Senate Seat" if state else "Senate Seat"
    if state and district:
        return f"{state}-{district}"
    if state:
        return state
    return "House Seat"


def _group_identifiers(identifiers: list[dict]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for identifier in identifiers:
        key = identifier.get("identifier_type") or "other"
        grouped.setdefault(key, []).append(identifier.get("identifier_value") or "")
    return grouped


def _build_funding_summary(contributions: list[dict]) -> dict:
    total_amount = 0.0
    pac_amount = 0.0
    individual_amount = 0.0
    grassroots_amount = 0.0
    mid_amount = 0.0
    large_amount = 0.0

    pac_count = 0
    individual_count = 0
    grassroots_count = 0
    mid_count = 0
    large_count = 0

    industry_totals: dict[str, float] = {}
    industry_counts: dict[str, int] = {}
    donor_totals: dict[str, dict] = {}
    cycle_totals: dict[str, float] = {}

    for contribution in contributions:
        amount = float(contribution.get("amount") or 0.0)
        total_amount += amount

        ctype = (contribution.get("contribution_type") or "unknown").lower()
        if ctype == "pac":
            pac_count += 1
            pac_amount += amount
        else:
            individual_count += 1
            individual_amount += amount
            if amount <= 200:
                grassroots_count += 1
                grassroots_amount += amount
            elif amount >= 1000:
                large_count += 1
                large_amount += amount
            else:
                mid_count += 1
                mid_amount += amount

        industry = _clean_bucket_label(contribution.get("contributor_industry"), fallback="Unknown")
        industry_totals[industry] = industry_totals.get(industry, 0.0) + amount
        industry_counts[industry] = industry_counts.get(industry, 0) + 1

        donor = _clean_bucket_label(contribution.get("contributor_name"), fallback="Unknown donor")
        donor_entry = donor_totals.setdefault(
            donor,
            {
                "name": donor,
                "amount": 0.0,
                "count": 0,
                "industry": industry,
                "type": ctype,
            },
        )
        donor_entry["amount"] += amount
        donor_entry["count"] += 1

        cycle = str(contribution.get("election_cycle") or "Unknown")
        cycle_totals[cycle] = cycle_totals.get(cycle, 0.0) + amount

    top_industries = [
        {
            "label": label,
            "amount": round(amount, 2),
            "count": industry_counts.get(label, 0),
            "share_pct": round(amount / total_amount * 100, 1) if total_amount else 0.0,
        }
        for label, amount in sorted(industry_totals.items(), key=lambda item: item[1], reverse=True)[:8]
    ]

    top_donors = sorted(
        donor_totals.values(),
        key=lambda item: (item["amount"], item["count"]),
        reverse=True,
    )[:10]
    for donor in top_donors:
        donor["amount"] = round(float(donor["amount"]), 2)

    cycles = [
        {"cycle": cycle, "amount": round(amount, 2)}
        for cycle, amount in sorted(cycle_totals.items(), key=lambda item: item[0], reverse=True)
    ]

    latest_date = max((c.get("contribution_date") or "" for c in contributions), default=None)

    return {
        "count": len(contributions),
        "total_amount": round(total_amount, 2),
        "latest_date": latest_date,
        "industry_count": len(industry_totals),
        "top_industries": top_industries,
        "top_donors": top_donors,
        "cycles": cycles,
        "mix": {
            "pac": {"count": pac_count, "amount": round(pac_amount, 2)},
            "individual": {"count": individual_count, "amount": round(individual_amount, 2)},
            "grassroots": {"count": grassroots_count, "amount": round(grassroots_amount, 2)},
            "mid": {"count": mid_count, "amount": round(mid_amount, 2)},
            "large": {"count": large_count, "amount": round(large_amount, 2)},
        },
        "notes": {
            "grassroots_definition": "Individual contributions of $200 or less.",
            "large_definition": "Individual contributions of $1,000 or more.",
            "sector_definition": "Uses FEC contributor industry labels as a proxy for funding sectors.",
        },
    }


def _clean_bucket_label(value: Optional[str], fallback: str) -> str:
    cleaned = " ".join((value or "").split()).strip(" -")
    return cleaned if cleaned else fallback
