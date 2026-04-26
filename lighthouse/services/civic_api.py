"""
Google Civic Information API client.
Used for ZIP-code-to-representative lookup and upcoming election ballot data.
Docs: https://developers.google.com/civic-information/docs/v2
"""
import logging
from typing import Optional

import requests

log = logging.getLogger(__name__)

CIVIC_BASE = "https://civicinfo.googleapis.com/civicinfo/v2"


class CivicApiError(Exception):
    pass


class CivicApiClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "Lighthouse/1.0"

    def get_location_from_zip(self, address: str) -> dict:
        """Resolve a ZIP/address to its state, city, and OCD divisions."""
        return self._get("divisionsByAddress", {"address": address})

    def get_upcoming_elections(self) -> list[dict]:
        """List of upcoming elections tracked by the Civic API."""
        data = self._get("elections", {})
        elections = data.get("elections", [])
        # Filter out the permanent test election (id "2000")
        return [e for e in elections if e.get("id") != "2000"]

    def get_voter_info(self, address: str, election_id: str) -> dict:
        """Detailed ballot for a specific address and election ID."""
        return self._get("voterinfo", {"address": address, "electionId": election_id})

    def _get(self, path: str, params: dict) -> dict:
        params = {**params, "key": self.api_key}
        try:
            r = self._session.get(f"{CIVIC_BASE}/{path}", params=params, timeout=10)
            if r.status_code == 400:
                msg = r.json().get("error", {}).get("message", "Bad request")
                raise CivicApiError(msg)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            log.warning("Civic API error (%s): %s", path, exc)
            raise CivicApiError(str(exc)) from exc


def parse_location(raw: dict) -> dict:
    """Extract state code and city from a divisionsByAddress response."""
    addr = raw.get("normalizedInput", {})
    state = (addr.get("state") or "").upper()
    city = addr.get("city") or ""
    zip_code = addr.get("zip") or ""
    normalized = ", ".join(filter(None, [city, state, zip_code]))
    return {
        "state": state,
        "city": city,
        "zip": zip_code,
        "normalized_address": normalized,
    }


def parse_voter_info(raw: dict) -> dict:
    """Extract contests and candidates from a voterinfo response."""
    contests = []
    for contest in raw.get("contests", []):
        candidates = [
            {
                "name": c.get("name", ""),
                "party": _clean_party(c.get("party", "")),
                "party_full": c.get("party", ""),
                "candidate_url": c.get("candidateUrl", ""),
                "photo_url": c.get("photoUrl", ""),
            }
            for c in contest.get("candidates", [])
        ]
        contests.append({
            "office": contest.get("office", ""),
            "level": _map_level(contest.get("level", [])),
            "type": contest.get("type", ""),
            "district": contest.get("district", {}).get("name", ""),
            "candidates": candidates,
            "referendumTitle": contest.get("referendumTitle", ""),
            "referendumSubtitle": contest.get("referendumSubtitle", ""),
            "is_referendum": contest.get("type") == "Referendum",
        })

    election = raw.get("election", {})
    return {
        "election_name": election.get("name", ""),
        "election_day": election.get("electionDay", ""),
        "contests": contests,
        "contest_count": len(contests),
    }


def _map_level(levels) -> str:
    if isinstance(levels, str):
        levels = [levels]
    if "country" in levels:
        return "federal"
    if "administrativeArea1" in levels:
        return "state"
    return "local"


def _clean_party(party: str) -> str:
    return {
        "Democratic": "D", "Democratic Party": "D",
        "Republican": "R", "Republican Party": "R",
        "Independent": "I", "Nonpartisan": "",
        "Green": "G", "Libertarian": "L",
    }.get(party, party[:1] if party else "")
