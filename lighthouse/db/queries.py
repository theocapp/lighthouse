"""
Named query helpers used by the detection engine and reporting layer.
All queries return plain dicts (not ORM objects) for portability.
"""
from typing import Optional

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from .models import (
    Asset, Bill, BillCosponsor, CampaignContribution, CommitteeMembership,
    Conflict, FinancialDisclosure, IngestionLog, Member, MemberIdentifier, MemberVote,
    StockTransaction, Vote,
)


def get_members(session: Session, bioguide_id: Optional[str] = None) -> list[Member]:
    q = session.query(Member).filter(Member.is_active.is_(True))
    if bioguide_id:
        q = q.filter(Member.bioguide_id == bioguide_id)
    return q.all()


def get_member_votes_with_bills(session: Session, bioguide_id: str) -> list[dict]:
    rows = (
        session.query(MemberVote, Vote, Bill)
        .join(Vote, MemberVote.vote_id == Vote.vote_id)
        .outerjoin(Bill, Vote.bill_id == Bill.bill_id)
        .filter(MemberVote.bioguide_id == bioguide_id)
        .all()
    )
    return [
        {
            "vote_id": mv.vote_id,
            "bill_id": v.bill_id,
            "position": mv.position,
            "vote_date": str(v.vote_date) if v.vote_date else None,
            "policy_area": b.policy_area if b else None,
            "subjects_json": b.subjects_json if b else "[]",
            "vote_source_url": v.source_url,
            "bill_source_url": b.govinfo_url if b else None,
            "bill_title": (b.short_title or b.title) if b else None,
        }
        for mv, v, b in rows
    ]


def get_all_votes_with_bills(session: Session, congress: int) -> list[dict]:
    rows = (
        session.query(Vote, Bill)
        .outerjoin(Bill, Vote.bill_id == Bill.bill_id)
        .filter(Vote.congress == congress)
        .all()
    )
    return [
        {
            "vote_id": v.vote_id,
            "bill_id": v.bill_id,
            "vote_date": str(v.vote_date) if v.vote_date else None,
            "policy_area": b.policy_area if b else None,
            "subjects_json": b.subjects_json if b else "[]",
            "vote_source_url": v.source_url,
            "bill_source_url": b.govinfo_url if b else None,
            "bill_title": (b.short_title or b.title) if b else None,
        }
        for v, b in rows
    ]


def get_member_assets(
    session: Session, bioguide_id: str, min_value: float = 1000.0
) -> list[dict]:
    rows = (
        session.query(Asset, FinancialDisclosure)
        .join(FinancialDisclosure, Asset.disclosure_id == FinancialDisclosure.id)
        .filter(Asset.bioguide_id == bioguide_id)
        .filter((Asset.value_max.is_(None)) | (Asset.value_max >= min_value))
        .all()
    )
    return [
        {
            "id": asset.id,
            "asset_name": asset.asset_name,
            "asset_type": asset.asset_type,
            "ticker": asset.ticker,
            "value_min": float(asset.value_min) if asset.value_min else None,
            "value_max": float(asset.value_max) if asset.value_max else None,
            "owner": asset.owner,
            "year": asset.year,
            "sector": asset.sector,
            "disclosure_id": disclosure.id,
            "disclosure_source": disclosure.source,
            "disclosure_source_url": disclosure.source_url,
            "disclosure_filed_date": str(disclosure.filed_date) if disclosure.filed_date else None,
            "disclosure_raw_file_path": disclosure.raw_file_path,
        }
        for asset, disclosure in rows
    ]


def get_member_transactions(session: Session, bioguide_id: str) -> list[dict]:
    rows = (
        session.query(StockTransaction)
        .filter(StockTransaction.bioguide_id == bioguide_id)
        .order_by(StockTransaction.transaction_date)
        .all()
    )
    return [
        {
            "id": t.id,
            "ticker": t.ticker,
            "asset_name": t.asset_name,
            "transaction_date": str(t.transaction_date) if t.transaction_date else None,
            "disclosure_date": str(t.disclosure_date) if t.disclosure_date else None,
            "transaction_type": t.transaction_type,
            "amount_min": float(t.amount_min) if t.amount_min else None,
            "amount_max": float(t.amount_max) if t.amount_max else None,
            "owner": t.owner,
            "sector": t.sector,
            "source": t.source,
        }
        for t in rows
    ]


def get_member_vote_stats(session: Session, bioguide_id: str) -> dict:
    """Count votes by position for participation stats."""
    rows = (
        session.query(MemberVote.position, func.count(MemberVote.vote_id))
        .filter(MemberVote.bioguide_id == bioguide_id)
        .group_by(MemberVote.position)
        .all()
    )
    stats: dict[str, int] = {"total": 0, "Yea": 0, "Nay": 0, "Not Voting": 0, "Present": 0}
    for position, count in rows:
        key = position if position in stats else "Present"
        stats[key] = stats.get(key, 0) + count
        stats["total"] += count
    participated = stats["Yea"] + stats["Nay"] + stats["Present"]
    stats["participation_rate"] = round(participated / stats["total"] * 100, 1) if stats["total"] else 0
    return stats


def get_member_recent_votes(
    session: Session, bioguide_id: str, limit: int = 25
) -> list[dict]:
    """Recent votes for a member with associated bill info."""
    rows = (
        session.query(MemberVote, Vote, Bill)
        .join(Vote, MemberVote.vote_id == Vote.vote_id)
        .outerjoin(Bill, Vote.bill_id == Bill.bill_id)
        .filter(MemberVote.bioguide_id == bioguide_id)
        .order_by(Vote.vote_date.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "vote_id": mv.vote_id,
            "position": mv.position,
            "vote_date": str(v.vote_date)[:10] if v.vote_date else None,
            "question": v.question,
            "result": v.result,
            "bill_id": v.bill_id,
            "bill_title": b.short_title or b.title if b else None,
            "policy_area": b.policy_area if b else None,
        }
        for mv, v, b in rows
    ]


def get_committee_memberships(
    session: Session, bioguide_id: str, congress: int
) -> list[dict]:
    query = session.query(CommitteeMembership).filter(
        CommitteeMembership.bioguide_id == bioguide_id,
    )
    if congress and congress > 0:
        query = query.filter(CommitteeMembership.congress == congress)
    rows = (
        query.order_by(
            CommitteeMembership.congress.desc(),
            CommitteeMembership.committee_name.asc(),
        )
        .all()
    )
    return [
        {
            "committee_code": c.committee_code,
            "committee_name": c.committee_name,
            "role": c.role,
            "congress": c.congress,
        }
        for c in rows
    ]


def get_member_identifiers(session: Session, bioguide_id: str) -> list[dict]:
    rows = (
        session.query(MemberIdentifier)
        .filter(MemberIdentifier.bioguide_id == bioguide_id)
        .order_by(
            MemberIdentifier.identifier_type.asc(),
            MemberIdentifier.is_primary.desc(),
            MemberIdentifier.identifier_value.asc(),
        )
        .all()
    )
    return [
        {
            "identifier_type": row.identifier_type,
            "identifier_value": row.identifier_value,
            "is_primary": bool(row.is_primary),
            "source": row.source,
        }
        for row in rows
    ]


def get_contributions(session: Session, bioguide_id: str) -> list[dict]:
    rows = (
        session.query(CampaignContribution)
        .filter(CampaignContribution.bioguide_id == bioguide_id)
        .order_by(
            CampaignContribution.amount.desc().nullslast(),
            CampaignContribution.contribution_date.desc().nullslast(),
        )
        .all()
    )
    return [
        {
            "id": c.id,
            "contributor_name": c.contributor_name,
            "contributor_employer": c.contributor_employer,
            "contributor_industry": c.contributor_industry,
            "amount": float(c.amount) if c.amount else None,
            "contribution_date": str(c.contribution_date) if c.contribution_date else None,
            "election_cycle": c.election_cycle,
            "contribution_type": c.contribution_type,
            "fec_committee_id": c.fec_committee_id,
        }
        for c in rows
    ]


def get_member_fec_ids(session: Session, bioguide_id: str) -> list[str]:
    rows = (
        session.query(MemberIdentifier)
        .filter(
            MemberIdentifier.bioguide_id == bioguide_id,
            MemberIdentifier.identifier_type == "fec_candidate_id",
        )
        .order_by(MemberIdentifier.is_primary.desc(), MemberIdentifier.identifier_value.asc())
        .all()
    )
    values = [row.identifier_value for row in rows if row.identifier_value]
    if values:
        return values

    member = session.get(Member, bioguide_id)
    if member and member.fec_candidate_id:
        return [member.fec_candidate_id]
    return []


def get_sponsored_bills(session: Session, bioguide_id: str) -> list[dict]:
    rows = (
        session.query(Bill)
        .filter(Bill.sponsor_bioguide == bioguide_id)
        .all()
    )
    return [_bill_to_dict(b) for b in rows]


def get_cosponsored_bills(session: Session, bioguide_id: str) -> list[dict]:
    rows = (
        session.query(BillCosponsor)
        .filter(BillCosponsor.bioguide_id == bioguide_id)
        .all()
    )
    return [{"bill_id": r.bill_id, "cosponsor_date": str(r.cosponsor_date) if r.cosponsor_date else None} for r in rows]


def get_bills_by_ids(session: Session, bill_ids: list[str]) -> dict[str, dict]:
    if not bill_ids:
        return {}
    rows = session.query(Bill).filter(Bill.bill_id.in_(bill_ids)).all()
    return {b.bill_id: _bill_to_dict(b) for b in rows}


def _bill_to_dict(b: Bill) -> dict:
    return {
        "bill_id": b.bill_id,
        "title": b.title,
        "short_title": b.short_title,
        "policy_area": b.policy_area,
        "subjects_json": b.subjects_json or "[]",
        "industries_json": b.industries_json or "[]",
        "sponsor_bioguide": b.sponsor_bioguide,
        "introduced_date": str(b.introduced_date) if b.introduced_date else None,
        "govinfo_url": b.govinfo_url,
    }


def get_conflicts_for_member(session: Session, bioguide_id: str) -> list[dict]:
    rows = (
        session.query(Conflict)
        .filter(Conflict.bioguide_id == bioguide_id)
        .order_by(Conflict.score.desc())
        .all()
    )
    return [
        {
            "id": c.id,
            "conflict_type": c.conflict_type,
            "score": c.score,
            "confidence": c.confidence,
            "vote_id": c.vote_id,
            "bill_id": c.bill_id,
            "asset_id": c.asset_id,
            "transaction_id": c.transaction_id,
            "contribution_id": c.contribution_id,
            "evidence_summary": c.evidence_summary,
            "detail_json": c.detail_json,
            "detected_at": str(c.detected_at) if c.detected_at else None,
        }
        for c in rows
    ]


def upsert_member(session: Session, data: dict) -> Member:
    obj = session.get(Member, data["bioguide_id"])
    if obj is None:
        obj = Member(**data)
        session.add(obj)
    else:
        for k, v in data.items():
            setattr(obj, k, v)
    return obj


def replace_member_identifiers(
    session: Session,
    bioguide_id: str,
    identifiers: list[dict],
    source: str,
):
    session.query(MemberIdentifier).filter(
        MemberIdentifier.bioguide_id == bioguide_id,
        MemberIdentifier.source == source,
    ).delete()

    for identifier in identifiers:
        session.add(MemberIdentifier(
            bioguide_id=bioguide_id,
            identifier_type=identifier["identifier_type"],
            identifier_value=identifier["identifier_value"],
            is_primary=bool(identifier.get("is_primary")),
            source=identifier.get("source", source),
        ))


def upsert_bill(session: Session, data: dict) -> Bill:
    obj = session.get(Bill, data["bill_id"])
    if obj is None:
        obj = Bill(**data)
        session.add(obj)
    else:
        for k, v in data.items():
            setattr(obj, k, v)
    return obj


# ---------------------------------------------------------------------------
# Web-specific queries
# ---------------------------------------------------------------------------

def get_dashboard_stats(session: Session) -> dict:
    """Aggregate counts for the dashboard summary cards."""
    from sqlalchemy import func

    total_members = session.query(func.count(Member.bioguide_id)).scalar() or 0
    total_conflicts = session.query(func.count(Conflict.id)).scalar() or 0
    high = session.query(func.count(Conflict.id)).filter(Conflict.confidence == "high").scalar() or 0
    medium = session.query(func.count(Conflict.id)).filter(Conflict.confidence == "medium").scalar() or 0
    low = session.query(func.count(Conflict.id)).filter(Conflict.confidence == "low").scalar() or 0
    total_trades = session.query(func.count(StockTransaction.id)).scalar() or 0
    total_assets = session.query(func.count(Asset.id)).scalar() or 0

    # Members with at least one medium- or high-confidence conflict
    flagged = (
        session.query(func.count(func.distinct(Conflict.bioguide_id)))
        .filter(Conflict.confidence.in_(["high", "medium"]))
        .scalar() or 0
    )

    # Breakdown by conflict type
    type_counts = (
        session.query(Conflict.conflict_type, func.count(Conflict.id))
        .group_by(Conflict.conflict_type)
        .all()
    )

    # Breakdown by sector (from detail_json — approximation via asset sector)
    sector_counts = (
        session.query(Asset.sector, func.count(Asset.id))
        .filter(Asset.sector.is_not(None))
        .group_by(Asset.sector)
        .order_by(func.count(Asset.id).desc())
        .limit(8)
        .all()
    )

    return {
        "total_members": total_members,
        "total_conflicts": total_conflicts,
        "high_confidence": high,
        "medium_confidence": medium,
        "low_confidence": low,
        "total_trades": total_trades,
        "total_assets": total_assets,
        "flagged_members": flagged,
        "by_type": {t: c for t, c in type_counts},
        "by_sector": {s: c for s, c in sector_counts if s},
        "data_coverage": get_data_coverage(session),
    }


def get_members_with_scores(
    session: Session,
    chamber: Optional[str] = None,
    party: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: str = "score",
    limit: int = 600,
) -> list[dict]:
    """
    Members list enriched with conflict counts and max score.
    Used by the /members page.
    """
    from sqlalchemy import func, case

    q = (
        session.query(
            Member,
            func.count(Conflict.id).label("conflict_count"),
            func.coalesce(func.max(Conflict.score), 0).label("max_score"),
            func.sum(case((Conflict.confidence == "high", 1), else_=0)).label("high_count"),
        )
        .outerjoin(Conflict, Conflict.bioguide_id == Member.bioguide_id)
        .filter(Member.is_active.is_(True))
        .group_by(Member.bioguide_id)
    )

    if chamber:
        q = q.filter(Member.chamber == chamber)
    if party:
        q = q.filter(Member.party == party)
    if search:
        q = q.filter(Member.full_name.ilike(f"%{search}%"))

    if sort_by == "score":
        q = q.order_by(func.coalesce(func.max(Conflict.score), 0).desc())
    elif sort_by == "name":
        q = q.order_by(Member.last_name)
    elif sort_by == "conflicts":
        q = q.order_by(func.count(Conflict.id).desc())

    rows = q.limit(limit).all()
    return [
        {
            "bioguide_id": m.bioguide_id,
            "full_name": m.full_name,
            "party": m.party,
            "state": m.state,
            "district": m.district,
            "chamber": m.chamber,
            "profile_image_url": _profile_image_url(m.bioguide_id),
            "conflict_count": conflict_count,
            "max_score": round(float(max_score), 1),
            "high_count": int(high_count or 0),
        }
        for m, conflict_count, max_score, high_count in rows
    ]


def get_all_conflicts(
    session: Session,
    conflict_type: Optional[str] = None,
    confidence: Optional[str] = None,
    chamber: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 500,
) -> list[dict]:
    """All conflicts joined with member info — used by the /conflicts explorer."""
    q = (
        session.query(Conflict, Member)
        .join(Member, Conflict.bioguide_id == Member.bioguide_id)
        .order_by(Conflict.score.desc())
    )

    if conflict_type:
        q = q.filter(Conflict.conflict_type == conflict_type)
    if confidence:
        q = q.filter(Conflict.confidence == confidence)
    if chamber:
        q = q.filter(Member.chamber == chamber)
    if search:
        q = q.filter(Member.full_name.ilike(f"%{search}%"))

    rows = q.limit(limit).all()
    return [
        {
            "id": c.id,
            "bioguide_id": c.bioguide_id,
            "full_name": m.full_name,
            "party": m.party,
            "state": m.state,
            "chamber": m.chamber,
            "conflict_type": c.conflict_type,
            "score": round(c.score, 1),
            "confidence": c.confidence,
            "evidence_summary": c.evidence_summary,
            "detail_json": c.detail_json,
            "bill_id": c.bill_id,
            "vote_id": c.vote_id,
            "detected_at": str(c.detected_at)[:10] if c.detected_at else None,
        }
        for c, m in rows
    ]


def get_top_conflicts(session: Session, limit: int = 10) -> list[dict]:
    """Top N conflicts with one representative highest-scoring row per member."""
    from sqlalchemy import and_, func

    max_per_member = (
        session.query(
            Conflict.bioguide_id.label("bioguide_id"),
            func.max(Conflict.score).label("max_score"),
        )
        .group_by(Conflict.bioguide_id)
        .subquery()
    )

    rows = (
        session.query(Conflict, Member)
        .join(Member, Conflict.bioguide_id == Member.bioguide_id)
        .join(
            max_per_member,
            and_(
                Conflict.bioguide_id == max_per_member.c.bioguide_id,
                Conflict.score == max_per_member.c.max_score,
            ),
        )
        .order_by(Conflict.score.desc(), Conflict.detected_at.desc())
        .all()
    )

    # Keep the first row per member in score order to avoid duplicates on score ties.
    top: list[dict] = []
    seen: set[str] = set()
    for c, m in rows:
        if c.bioguide_id in seen:
            continue
        seen.add(c.bioguide_id)
        top.append(
            {
                "id": c.id,
                "bioguide_id": c.bioguide_id,
                "full_name": m.full_name,
                "party": m.party,
                "state": m.state,
                "chamber": m.chamber,
                "conflict_type": c.conflict_type,
                "score": round(c.score, 1),
                "confidence": c.confidence,
                "evidence_summary": c.evidence_summary,
                "detail_json": c.detail_json,
                "bill_id": c.bill_id,
                "vote_id": c.vote_id,
                "detected_at": str(c.detected_at)[:10] if c.detected_at else None,
            }
        )
        if len(top) >= limit:
            break

    return top


def get_recent_transactions(session: Session, limit: int = 20) -> list[dict]:
    """Most recent stock trades across all members — for dashboard."""
    rows = (
        session.query(StockTransaction, Member)
        .join(Member, StockTransaction.bioguide_id == Member.bioguide_id)
        .order_by(StockTransaction.transaction_date.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": t.id,
            "bioguide_id": t.bioguide_id,
            "full_name": m.full_name,
            "party": m.party,
            "ticker": t.ticker,
            "asset_name": t.asset_name,
            "transaction_type": t.transaction_type,
            "amount_min": float(t.amount_min) if t.amount_min else None,
            "amount_max": float(t.amount_max) if t.amount_max else None,
            "transaction_date": str(t.transaction_date) if t.transaction_date else None,
        }
        for t, m in rows
    ]


def _profile_image_url(bioguide_id: str) -> str:
    initial = (bioguide_id or "X")[:1].upper()
    return f"https://bioguide.congress.gov/bioguide/photo/{initial}/{bioguide_id.upper()}.jpg"


def get_data_coverage(session: Session) -> dict:
    house_votes = session.query(func.count(Vote.vote_id)).filter(Vote.chamber == "house").scalar() or 0
    senate_votes = session.query(func.count(Vote.vote_id)).filter(Vote.chamber == "senate").scalar() or 0
    house_disclosures = (
        session.query(func.count(FinancialDisclosure.id))
        .filter(FinancialDisclosure.source == "house")
        .scalar()
        or 0
    )
    senate_disclosures = (
        session.query(func.count(FinancialDisclosure.id))
        .filter(FinancialDisclosure.source == "senate")
        .scalar()
        or 0
    )
    fec_contributions = session.query(func.count(CampaignContribution.id)).scalar() or 0

    ingestion_rows = session.query(IngestionLog).order_by(IngestionLog.source).all()

    return {
        "house_votes": int(house_votes),
        "senate_votes": int(senate_votes),
        "senate_votes_status": "partial_or_unavailable" if senate_votes == 0 else "loaded",
        "house_disclosures": int(house_disclosures),
        "senate_disclosures": int(senate_disclosures),
        "fec_contributions": int(fec_contributions),
        "last_ingestion_by_source": [
            {
                "source": row.source,
                "last_run": str(row.last_run) if row.last_run else None,
                "status": row.status,
                "records_added": row.records_added,
                "records_updated": row.records_updated,
            }
            for row in ingestion_rows
        ],
    }
