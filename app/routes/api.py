"""JSON REST API routes — for programmatic access and CSV/JSON exports."""
import json
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from ..deps import get_session
from lighthouse.db import queries as q
from lighthouse.reporting.member_report import build_report

router = APIRouter(prefix="/api")


@router.get("/stats")
def stats(session: Session = Depends(get_session)):
    return q.get_dashboard_stats(session)


@router.get("/members")
def members(
    chamber: Optional[str] = None,
    party: Optional[str] = None,
    search: Optional[str] = None,
    sort: str = "score",
    session: Session = Depends(get_session),
):
    return q.get_members_with_scores(session, chamber=chamber, party=party, search=search, sort_by=sort)


@router.get("/members/{bioguide_id}/report")
def member_report(bioguide_id: str, session: Session = Depends(get_session)):
    report = build_report(session, bioguide_id.upper())
    if not report:
        raise HTTPException(status_code=404, detail=f"Member {bioguide_id} not found")
    return JSONResponse(content=report)


@router.get("/members/{bioguide_id}/conflicts")
def member_conflicts(bioguide_id: str, session: Session = Depends(get_session)):
    conflicts = q.get_conflicts_for_member(session, bioguide_id.upper())
    if conflicts is None:
        raise HTTPException(status_code=404, detail=f"Member {bioguide_id} not found")
    return conflicts


@router.get("/conflicts")
def conflicts(
    confidence: Optional[str] = None,
    type: Optional[str] = None,
    chamber: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 500,
    session: Session = Depends(get_session),
):
    return q.get_all_conflicts(
        session,
        conflict_type=type,
        confidence=confidence,
        chamber=chamber,
        search=search,
        limit=min(limit, 2000),
    )


@router.get("/trades")
def trades(
    search: Optional[str] = None,
    limit: int = 500,
    session: Session = Depends(get_session),
):
    from lighthouse.db.models import StockTransaction, Member
    query = (
        session.query(StockTransaction, Member)
        .join(Member, StockTransaction.bioguide_id == Member.bioguide_id)
        .order_by(StockTransaction.transaction_date.desc())
    )
    if search:
        query = query.filter(
            Member.full_name.ilike(f"%{search}%") |
            StockTransaction.ticker.ilike(f"%{search}%")
        )
    rows = query.limit(min(limit, 2000)).all()
    return [
        {
            "bioguide_id": t.bioguide_id,
            "full_name": m.full_name,
            "party": m.party,
            "state": m.state,
            "ticker": t.ticker,
            "asset_name": t.asset_name,
            "transaction_type": t.transaction_type,
            "amount_min": float(t.amount_min) if t.amount_min else None,
            "amount_max": float(t.amount_max) if t.amount_max else None,
            "transaction_date": str(t.transaction_date) if t.transaction_date else None,
            "owner": t.owner,
            "source": t.source,
        }
        for t, m in rows
    ]
