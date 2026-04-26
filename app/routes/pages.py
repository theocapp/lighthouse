import json
import re
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from ..deps import get_session
from lighthouse.db import queries as q
from lighthouse.db.models import StockTransaction, Member
from lighthouse.reporting.member_report import build_report
from lighthouse.services.civic_api import CivicApiClient, CivicApiError, parse_location, parse_voter_info
from lighthouse.config import config as _cfg

_STATE_NAME_TO_CODE = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}
_VALID_STATE_CODES = set(_STATE_NAME_TO_CODE.values())


def _resolve_state_code(text: str) -> Optional[str]:
    text = text.strip()
    if len(text) == 2 and text.upper() in _VALID_STATE_CODES:
        return text.upper()
    return _STATE_NAME_TO_CODE.get(text.lower())

router = APIRouter()

TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Register a fromjson filter so templates can parse detail_json strings
templates.env.filters["fromjson"] = lambda s: json.loads(s) if s else {}


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request, session: Session = Depends(get_session)):
    stats = q.get_dashboard_stats(session)
    top_conflicts = q.get_top_conflicts(session, limit=10)
    recent_trades = q.get_recent_transactions(session, limit=15)
    featured_members = q.get_members_with_scores(session, sort_by="score", limit=12)
    return templates.TemplateResponse(request, "dashboard.html", {
        "active_page": "dashboard",
        "stats": stats,
        "top_conflicts": top_conflicts,
        "recent_trades": recent_trades,
        "featured_members": featured_members,
    })


@router.get("/members", response_class=HTMLResponse)
def members_list(
    request: Request,
    chamber: Optional[str] = None,
    party: Optional[str] = None,
    search: Optional[str] = None,
    sort: Optional[str] = "score",
    session: Session = Depends(get_session),
):
    members = q.get_members_with_scores(
        session, chamber=chamber, party=party, search=search, sort_by=sort
    )
    return templates.TemplateResponse(request, "members.html", {
        "active_page": "members",
        "members": members,
        "chamber": chamber or "",
        "party": party or "",
        "search": search or "",
        "sort": sort or "score",
    })


@router.get("/members/{bioguide_id}", response_class=HTMLResponse)
def member_detail(
    request: Request,
    bioguide_id: str,
    session: Session = Depends(get_session),
):
    report = build_report(session, bioguide_id.upper())
    if not report:
        return templates.TemplateResponse(request, "404.html", {}, status_code=404)

    # Pre-parse detail_json on each conflict so templates can access it as a dict
    for c in report.get("conflicts", []):
        c["detail"] = json.loads(c.get("detail_json") or "{}")

    return templates.TemplateResponse(request, "member_detail.html", {
        "active_page": "members",
        "report": report,
    })


@router.get("/conflicts", response_class=HTMLResponse)
def conflicts_explorer(
    request: Request,
    confidence: Optional[str] = None,
    type: Optional[str] = None,
    chamber: Optional[str] = None,
    search: Optional[str] = None,
    session: Session = Depends(get_session),
):
    conflicts = q.get_all_conflicts(
        session,
        conflict_type=type,
        confidence=confidence,
        chamber=chamber,
        search=search,
        limit=500,
    )

    all_conflicts = q.get_all_conflicts(session, limit=2000)
    type_counts: dict[str, int] = {}
    for c in all_conflicts:
        type_counts[c["conflict_type"]] = type_counts.get(c["conflict_type"], 0) + 1

    return templates.TemplateResponse(request, "conflicts.html", {
        "active_page": "conflicts",
        "conflicts": conflicts,
        "type_counts": type_counts,
        "confidence": confidence or "",
        "conflict_type": type or "",
        "chamber": chamber or "",
        "search": search or "",
    })


@router.get("/elections", response_class=HTMLResponse)
def elections(
    request: Request,
    search: Optional[str] = None,
    cycle: Optional[str] = None,
    level: Optional[str] = None,
    session: Session = Depends(get_session),
):
    cycle_int: Optional[int] = int(cycle) if cycle and cycle.strip().isdigit() else None

    result_type = None
    civic_data = None
    civic_error = None
    state_races = None
    member_results = None
    state_code = None
    available_cycles = q.get_available_election_cycles(session)

    query = (search or "").strip()

    if query:
        if re.match(r"^\d{5}$", query):
            result_type = "zip"
            api_key = _cfg.api_keys.google_civic
            if not api_key:
                civic_error = "Google Civic API key not configured."
            else:
                try:
                    client = CivicApiClient(api_key)
                    raw_loc = client.get_location_from_zip(query)
                    location = parse_location(raw_loc)
                    zip_state = location.get("state")
                    zip_members = []
                    if zip_state:
                        zip_members = q.get_members_with_scores(session, limit=50)
                        zip_members = [m for m in zip_members if m.get("state") == zip_state]
                    civic_data = {
                        "location": location,
                        "zip_members": zip_members,
                    }
                    upcoming = client.get_upcoming_elections()
                    for election in upcoming:
                        try:
                            raw_vi = client.get_voter_info(query, election["id"])
                            vi = parse_voter_info(raw_vi)
                            if vi.get("contests"):
                                civic_data["voter_info"] = vi
                                civic_data["election_name"] = election.get("name", "")
                                civic_data["election_day"] = election.get("electionDay", "")
                                break
                        except CivicApiError:
                            continue
                except CivicApiError as exc:
                    civic_error = str(exc)

        else:
            state_code = _resolve_state_code(query)
            if state_code:
                result_type = "state"
                state_races = q.get_elections_for_state(
                    session, state_code, cycle=cycle_int, office_level=level or None
                )
            else:
                result_type = "member"
                member_results = q.get_members_with_scores(session, search=query, limit=20)
                for m in member_results:
                    m["election_history"] = q.get_election_history_for_member(
                        session, m["bioguide_id"]
                    )

    return templates.TemplateResponse(request, "elections.html", {
        "active_page": "elections",
        "query": query,
        "result_type": result_type,
        "civic_data": civic_data,
        "civic_error": civic_error,
        "state_races": state_races,
        "state_code": state_code,
        "member_results": member_results,
        "cycle": cycle or "",
        "level": level or "",
        "available_cycles": available_cycles,
    })


@router.get("/trades", response_class=HTMLResponse)
def trades_page(
    request: Request,
    search: Optional[str] = None,
    type: Optional[str] = None,
    session: Session = Depends(get_session),
):
    query = (
        session.query(StockTransaction, Member)
        .join(Member, StockTransaction.bioguide_id == Member.bioguide_id)
        .order_by(StockTransaction.transaction_date.desc())
    )
    if type:
        query = query.filter(StockTransaction.transaction_type == type)
    if search:
        query = query.filter(
            Member.full_name.ilike(f"%{search}%") |
            StockTransaction.ticker.ilike(f"%{search}%")
        )

    rows = query.limit(500).all()
    trades = [
        {
            "id": t.id,
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

    return templates.TemplateResponse(request, "trades.html", {
        "active_page": "trades",
        "trades": trades,
        "search": search or "",
        "txn_type": type or "",
    })
