import csv
import json
from pathlib import Path


def write(report: dict, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = _flatten(report)
    if not rows:
        output_path.write_text("")
        return output_path

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    return output_path


def _flatten(report: dict) -> list[dict]:
    member = report.get("member", {})
    rows = []
    for c in report.get("conflicts", []):
        detail = json.loads(c.get("detail_json") or "{}")
        rows.append({
            "bioguide_id": member.get("bioguide_id"),
            "member_name": member.get("full_name"),
            "party": member.get("party"),
            "state": member.get("state"),
            "chamber": member.get("chamber"),
            "conflict_type": c.get("conflict_type"),
            "signal_score": c.get("score"),
            "confidence": c.get("confidence"),
            "signal_strength": detail.get("signal_strength"),
            "evidence_summary": c.get("evidence_summary"),
            "vote_id": c.get("vote_id"),
            "bill_id": c.get("bill_id"),
            "evidence_tier": detail.get("evidence_tier"),
            "match_reason": detail.get("match_reason"),
            "sector": detail.get("sector"),
            "ticker": detail.get("ticker"),
            "asset_name": detail.get("asset_name"),
            "asset_value_max": detail.get("value_max"),
            "trade_date": detail.get("transaction_date"),
            "vote_date": detail.get("vote_date"),
            "gap_days": detail.get("gap_days"),
            "source_quality": detail.get("source_quality"),
            "bill_source_url": detail.get("bill_source_url"),
            "vote_source_url": detail.get("vote_source_url"),
            "asset_source_url": detail.get("asset_source_url"),
            "detected_at": c.get("detected_at"),
        })
    return rows
