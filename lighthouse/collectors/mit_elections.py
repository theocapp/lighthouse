"""
Ingest election result data from MIT Election Data and Science Lab (MEDSL) CSVs.

Download data from: https://dataverse.harvard.edu/dataverse/medsl
Relevant datasets:
  - U.S. House 1976-2022: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2
  - U.S. Senate 1976-2020: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/PEJ5QU
  - U.S. President 1976-2020: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/42MVDX
  - U.S. Governor 1976-2020: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/DGUMFI

Usage:
    from lighthouse.collectors.mit_elections import ingest_house_csv, ingest_senate_csv
    ingest_house_csv(session, "./data/1976-2022-house.csv")
    ingest_senate_csv(session, "./data/1976-2022-senate.csv")
"""
import csv
import logging
from datetime import date
from pathlib import Path

from sqlalchemy.orm import Session

from ..db.models import ElectionCandidate, ElectionRace

log = logging.getLogger(__name__)

_PARTY_MAP = {
    "DEMOCRAT": "D", "DEMOCRATIC": "D",
    "REPUBLICAN": "R",
    "INDEPENDENT": "I",
    "LIBERTARIAN": "L",
    "GREEN": "G",
    "OTHER": "O",
}


def ingest_house_csv(session: Session, path: str) -> dict:
    """Ingest MIT house CSV (columns: year, state_po, district, stage, runoff, special, candidate, party, writein, candidatevotes, totalvotes)."""
    return _ingest_csv(session, path, office="U.S. Representative", office_level="federal", district_col="district")


def ingest_senate_csv(session: Session, path: str) -> dict:
    """Ingest MIT senate CSV."""
    return _ingest_csv(session, path, office="U.S. Senator", office_level="federal", district_col=None)


def ingest_governor_csv(session: Session, path: str) -> dict:
    """Ingest MIT governor CSV."""
    return _ingest_csv(session, path, office="Governor", office_level="state", district_col=None)


def ingest_president_csv(session: Session, path: str) -> dict:
    """Ingest MIT presidential CSV (state-level aggregates)."""
    return _ingest_csv(session, path, office="President", office_level="federal", district_col=None)


def _ingest_csv(
    session: Session,
    path: str,
    office: str,
    office_level: str,
    district_col: str | None,
) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Election CSV not found: {path}")

    races: dict[str, ElectionRace] = {}
    candidates_added = 0

    with open(p, newline="", encoding="utf-8") as f:
        lines = f.readlines()

    # Dataverse sometimes wraps every data row in outer double quotes with inner \" escaping.
    # Strip the outer quotes and unescape inner quotes so CSV parsing works correctly.
    if len(lines) > 1 and lines[1].startswith('"') and lines[1].rstrip("\n").endswith('"'):
        def _unwrap(line: str) -> str:
            stripped = line.rstrip("\n")
            if stripped.startswith('"') and stripped.endswith('"'):
                stripped = stripped[1:-1].replace('\\"', '"')
            return stripped + "\n"
        lines = [lines[0]] + [_unwrap(line) for line in lines[1:]]

    sample = "".join(lines[:20])
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t|")
        delimiter = dialect.delimiter
    except csv.Error:
        delimiter = ","

    import io
    reader = csv.DictReader(io.StringIO("".join(lines)), delimiter=delimiter)
    for row in reader:
            if (row.get("writein") or "").upper() == "TRUE":
                continue
            if not row.get("candidate") or not row.get("candidatevotes"):
                continue

            year = int(row["year"])
            state = (row.get("state_po") or "").upper()
            if not state:
                continue

            stage = _normalize_stage(row.get("stage", "GEN"))
            if (row.get("runoff") or "").upper() == "TRUE":
                stage = "runoff"
            special = (row.get("special") or "FALSE").upper() == "TRUE"
            district = str(row.get(district_col) or "").strip() if district_col else None

            source_key = f"mit_{office_level}_{year}_{state}_{district or 'statewide'}_{stage}"
            if special:
                source_key += "_special"

            if source_key not in races:
                existing = session.query(ElectionRace).filter_by(source_key=source_key).first()
                if existing:
                    races[source_key] = existing
                else:
                    race = ElectionRace(
                        cycle=year,
                        state=state,
                        office=office,
                        office_level=office_level,
                        district=district,
                        stage=stage,
                        special=special,
                        election_date=date(year, 11, 1),  # MIT doesn't provide exact date
                        total_votes=int(row.get("totalvotes") or 0) or None,
                        source="mit_election_lab",
                        source_key=source_key,
                    )
                    session.add(race)
                    session.flush()
                    races[source_key] = race

            race = races[source_key]
            # Update total_votes if we have a better value
            total = int(row.get("totalvotes") or 0)
            if total and not race.total_votes:
                race.total_votes = total

            party_raw = (row.get("party_simplified") or row.get("party") or "").upper()
            party = _PARTY_MAP.get(party_raw, party_raw[:1] if party_raw else "")
            votes = int(row.get("candidatevotes") or 0)
            vote_pct = round(votes / total * 100, 2) if total > 0 else None

            session.add(ElectionCandidate(
                race_id=race.id,
                candidate_name=_normalize_name(row["candidate"]),
                party=party,
                votes=votes,
                vote_pct=vote_pct,
            ))
            candidates_added += 1

    session.flush()

    # Mark winner (most votes) in general-stage races
    for race in races.values():
        if race.stage != "general":
            continue
        valid = [c for c in race.candidates if c.votes is not None]
        if valid:
            max(valid, key=lambda c: c.votes or 0).winner = True

    session.commit()
    log.info("Ingested %d candidates across %d races (%s)", candidates_added, len(races), office)
    return {"candidates_added": candidates_added, "races_added": len(races)}


def _normalize_stage(stage: str) -> str:
    return {"GEN": "general", "GENERAL": "general", "PRI": "primary", "PRIMARY": "primary",
            "RUN": "runoff", "RUNOFF": "runoff"}.get(stage.upper(), stage.lower())


def _normalize_name(name: str) -> str:
    name = name.strip()
    if "," in name:
        last, first = name.split(",", 1)
        name = f"{first.strip()} {last.strip()}"
    return name.title()
