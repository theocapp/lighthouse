"""
Conservative asset classification helpers.

These helpers infer a best-effort asset class and sector from disclosure text,
while preserving uncertainty when the match is weak or ambiguous.
"""
from __future__ import annotations

import re
from typing import Any, Optional

from .industry_map import ticker_to_sector


_COMPANY_ALIASES: list[tuple[str, str, str, str]] = [
    (r"\bapple\b", "AAPL", "information_technology", "high"),
    (r"\bmicrosoft\b", "MSFT", "information_technology", "high"),
    (r"\bamazon\b", "AMZN", "consumer_discretionary", "high"),
    (r"\balphabet\b|\bgoogle\b", "GOOGL", "communication_services", "high"),
    (r"\bmeta\b|\bmeta platforms\b", "META", "communication_services", "medium"),
    (r"\bnvidia\b", "NVDA", "information_technology", "high"),
    (r"\btesla\b", "TSLA", "consumer_discretionary", "high"),
    (r"\bjpmorgan chase\b|\bjpmorgan\b", "JPM", "financials", "high"),
    (r"\bbank of america\b", "BAC", "financials", "high"),
    (r"\bwells fargo\b", "WFC", "financials", "high"),
    (r"\berkshire hathaway\b", "BRK.B", "financials", "high"),
    (r"\bmorgan stanley\b", "MS", "financials", "high"),
    (r"\bgoldman sachs\b", "GS", "financials", "high"),
    (r"\bblackrock(?:,?\s+inc\.?|\s+incorporated)\b", "BLK", "financials", "medium"),
    (r"\bprudential financial\b", "PRU", "financials", "high"),
    (r"\bprincipal financial(?: group)?\b", "PFG", "financials", "high"),
    (r"\bjohnson and johnson\b|\bjohnson & johnson\b|\bj&j\b", "JNJ", "health_care", "high"),
    (r"\bpfizer\b", "PFE", "health_care", "high"),
    (r"\beli lilly\b", "LLY", "health_care", "high"),
    (r"\bunitedhealth\b", "UNH", "health_care", "high"),
    (r"\bexxon mobil\b|\bexxon\b", "XOM", "energy", "high"),
    (r"\bchevron\b", "CVX", "energy", "high"),
    (r"\blockheed martin\b", "LMT", "defense", "high"),
    (r"\bnorthrop grumman\b", "NOC", "defense", "high"),
    (r"\braytheon\b|\brtx\b", "RTX", "defense", "high"),
    (r"\bboeing\b", "BA", "industrials", "high"),
    (r"\bdisney\b", "DIS", "communication_services", "high"),
    (r"\bnetflix\b", "NFLX", "communication_services", "high"),
    (r"\bcomcast\b", "CMCSA", "communication_services", "high"),
    (r"\bverizon\b", "VZ", "communication_services", "high"),
    (r"\bat&t\b|\batt\b", "T", "communication_services", "medium"),
    (r"\bwalmart\b", "WMT", "consumer_staples", "high"),
    (r"\bcostco\b", "COST", "consumer_staples", "high"),
    (r"\bhome depot\b", "HD", "consumer_discretionary", "high"),
    (r"\bcoca[- ]cola\b|\bthe coca cola company\b", "KO", "consumer_staples", "high"),
    (r"\bpepsico\b", "PEP", "consumer_staples", "high"),
    (r"\bprocter and gamble\b|\bprocter & gamble\b|\bp&g\b", "PG", "consumer_staples", "high"),
    (r"\bintel\b", "INTC", "information_technology", "high"),
    (r"\badvanced micro devices\b|\bamd\b", "AMD", "information_technology", "high"),
]


_DIVERSIFIED_FUND_PATTERNS = [
    r"\bs&p 500\b",
    r"\btotal (?:stock )?market\b",
    r"\bindex fund\b",
    r"\bexchange[ -]traded fund\b",
    r"\betf\b",
    r"\bmutual fund\b",
    r"\btarget[ -]date\b",
    r"\bbond fund\b",
    r"\btreasury fund\b",
]

_DIVERSIFIED_PROVIDER_PATTERNS = [
    "vanguard",
    "fidelity",
    "blackrock",
    "ishares",
    "schwab",
    "charles schwab",
    "spdr",
    "s&p 500",
    "total market",
    "index fund",
    "exchange traded fund",
    "etf",
    "mutual fund",
    "target date",
    "bond fund",
    "treasury fund",
    "tiaa",
    "cref",
    "teachers insurance",
    "prudential",
    "principal",
    "american funds",
    "t. rowe price",
    "dimensional",
    "state street",
]

_DIVERSIFIED_PROVIDER_CONTEXT_PATTERNS = [
    *_DIVERSIFIED_FUND_PATTERNS,
    r"\bfund\b",
    r"\bindex\b",
    r"\bretirement\b",
    r"\b401\s*\(?k\)?\b",
    r"\b403\s*\(?b\)?\b",
    r"\bira\b",
]

_TREASURY_CONTEXT_PATTERNS = [
    r"\btreasury\b",
    r"\bu\.?s\.?\s+(?:government\s+)?(?:treasury\s+)?(?:bill|note|bond|security|securities)\b",
    r"\bunited states\s+(?:government\s+)?(?:treasury\s+)?(?:bill|note|bond|security|securities)\b",
    r"\bt[- ]?(?:bill|note|bond)s?\b",
]
_MUNICIPAL_BOND_PATTERNS = [r"\bmunicipal bond\b", r"\bmuni(?:cipal)? bond\b"]
_CORPORATE_BOND_PATTERNS = [r"\bcorporate bond\b", r"\bcorp(?:orate)? bond\b", r"\bcorporate note\b"]
_CASH_PATTERNS = [
    r"\bmoney market\b",
    r"\bsavings account\b",
    r"\bchecking account\b",
    r"\bdeposit account\b",
    r"\bcertificate of deposit\b",
    r"^cd$",
    r"\bbrokerage sweep\b",
    r"\bcash account\b",
    r"\bcash (?:and equivalents?|balance|holding|reserve|management)\b",
    r"^cash$",
]
_REAL_ESTATE_PATTERNS = [
    r"\breal estate\b",
    r"\brental property\b",
    r"\b(?:vacant|undeveloped|farm|timber|agricultural)\s+land\b",
    r"\bland parcel\b",
    r"\bfarm(?:land)?\b",
    r"\bapartment\b",
    r"\bcondo(?:minium)?\b",
    r"\bresidential property\b",
    r"\bcommercial property\b",
    r"\breit\b",
]
_TRUST_PATTERNS = [r"\bfamily trust\b", r"\brevocable trust\b", r"\bliving trust\b", r"\btrust account\b"]
_PRIVATE_BUSINESS_PATTERNS = [
    r"\bllc\b",
    r"\bl\.l\.c\.\b",
    r"\bl\.?p\.?\b",
    r"\blimited partnership\b",
    r"\bpartnership\b",
    r"\bprivate company\b",
    r"\bclosely held\b",
]


def classify_asset_record(asset: dict[str, Any]) -> dict[str, Any]:
    """Classify an asset dict and return merged classification metadata."""
    asset_name = _clean_text(str(asset.get("asset_name") or ""))
    ticker = _clean_ticker(asset.get("ticker"))
    existing_sector = _normalize_unknown(asset.get("sector"))
    existing_asset_class = _normalize_unknown(asset.get("asset_type"))

    classification = {
        "sector": existing_sector,
        "asset_class": existing_asset_class,
        "classification_confidence": "low",
        "classification_reason": "No strong classification match.",
        "matched_ticker": ticker,
        "is_diversified": False,
    }

    if ticker:
        sector = ticker_to_sector(ticker)
        if sector != "unknown":
            asset_class = "diversified_fund" if sector == "diversified" else "public_equity"
            classification.update({
                "sector": sector,
                "asset_class": asset_class,
                "classification_confidence": "high",
                "classification_reason": f"Ticker {ticker} maps to {sector}.",
                "is_diversified": sector == "diversified",
            })
            return _merge_classification(asset, classification)

    if not ticker and asset_name:
        diversified_match = _match_diversified_fund(asset_name)
        if diversified_match:
            high_confidence_match = _match_any(asset_name, _DIVERSIFIED_FUND_PATTERNS)
            classification.update({
                "sector": "diversified",
                "asset_class": "diversified_fund",
                "classification_confidence": "high" if high_confidence_match else "medium",
                "classification_reason": f"Diversified fund signal matched: {diversified_match}.",
                "is_diversified": True,
            })
            return _merge_classification(asset, classification)

        matched_company = _match_company_alias(asset_name)
        if matched_company:
            matched_ticker, sector, confidence = matched_company
            classification.update({
                "sector": sector,
                "asset_class": "public_equity",
                "classification_confidence": confidence,
                "classification_reason": f"Company alias matched to {matched_ticker}.",
                "matched_ticker": matched_ticker,
                "is_diversified": sector == "diversified",
            })
            return _merge_classification(asset, classification)

        fixed_income = _match_fixed_income(asset_name)
        if fixed_income:
            asset_class, sector, reason, confidence = fixed_income
            classification.update({
                "sector": sector,
                "asset_class": asset_class,
                "classification_confidence": confidence,
                "classification_reason": reason,
                "is_diversified": False,
            })
            return _merge_classification(asset, classification)

        real_estate = _match_any(asset_name, _REAL_ESTATE_PATTERNS)
        if real_estate:
            classification.update({
                "sector": "real_estate",
                "asset_class": "real_estate",
                "classification_confidence": "medium",
                "classification_reason": f"Real estate signal matched: {real_estate}.",
            })
            return _merge_classification(asset, classification)

        trust = _match_any(asset_name, _TRUST_PATTERNS)
        if trust:
            classification.update({
                "sector": "unknown",
                "asset_class": "trust",
                "classification_confidence": "medium",
                "classification_reason": f"Trust signal matched: {trust}.",
            })
            return _merge_classification(asset, classification)

        private_business = _match_any(asset_name, _PRIVATE_BUSINESS_PATTERNS)
        if private_business:
            classification.update({
                "sector": "unknown",
                "asset_class": "private_business",
                "classification_confidence": "medium",
                "classification_reason": f"Private business signal matched: {private_business}.",
            })
            return _merge_classification(asset, classification)

    return _merge_classification(asset, classification)


def _match_company_alias(asset_name: str) -> Optional[tuple[str, str, str]]:
    lower = asset_name.lower()
    for pattern, ticker, sector, confidence in _COMPANY_ALIASES:
        if re.search(pattern, lower):
            return ticker, sector, confidence
    return None


def _match_diversified_fund(asset_name: str) -> Optional[str]:
    fund_match = _match_any(asset_name, _DIVERSIFIED_FUND_PATTERNS)
    if fund_match:
        return fund_match

    provider_match = _match_any(asset_name, _DIVERSIFIED_PROVIDER_PATTERNS)
    if provider_match and _match_any(asset_name, _DIVERSIFIED_PROVIDER_CONTEXT_PATTERNS):
        return provider_match

    return None


def _match_fixed_income(asset_name: str) -> Optional[tuple[str, str, str, str]]:
    lower = asset_name.lower()

    muni = _match_any(lower, _MUNICIPAL_BOND_PATTERNS)
    if muni:
        return "municipal_bond", "fixed_income", f"Municipal bond signal matched: {muni}.", "high"

    corporate = _match_any(lower, _CORPORATE_BOND_PATTERNS)
    if corporate:
        return "corporate_bond", "fixed_income", f"Corporate bond signal matched: {corporate}.", "high"

    treasury = _match_any(lower, _TREASURY_CONTEXT_PATTERNS)
    if treasury:
        return "treasury", "fixed_income", f"Treasury signal matched: {treasury}.", "high"

    cash = _match_any(lower, _CASH_PATTERNS)
    if cash:
        asset_class = "money_market" if "money market" in lower else "cash_or_deposit"
        confidence = "high" if cash in {
            "money market",
            "brokerage sweep",
            "savings account",
            "checking account",
            "deposit account",
            "certificate of deposit",
        } else "medium"
        return asset_class, "cash", f"Cash or deposit signal matched: {cash}.", confidence

    return None


def _match_any(text: str, patterns: list[str]) -> Optional[str]:
    lower = text.lower()
    for pattern in patterns:
        if re.search(pattern, lower):
            return pattern.replace(r"\b", "").replace("|", " or ")
    return None


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _clean_ticker(value: Any) -> Optional[str]:
    ticker = str(value or "").strip().upper()
    return ticker or None


def _normalize_unknown(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "unknown"
    lower = text.lower()
    return "unknown" if lower in {"other", "unknown", "none", "null"} else lower


def _merge_classification(asset: dict[str, Any], classification: dict[str, Any]) -> dict[str, Any]:
    merged = dict(classification)
    existing_sector = _normalize_unknown(asset.get("sector"))
    existing_asset_class = _normalize_unknown(asset.get("asset_type"))

    if merged["sector"] in {"unknown", ""} and existing_sector not in {"unknown", "other", ""}:
        merged["sector"] = existing_sector
    if merged["asset_class"] in {"unknown", ""} and existing_asset_class not in {"unknown", "other", ""}:
        merged["asset_class"] = existing_asset_class

    if not merged.get("matched_ticker") and asset.get("ticker"):
        merged["matched_ticker"] = _clean_ticker(asset.get("ticker"))
    if merged["sector"] == "diversified":
        merged["is_diversified"] = True
    return merged


def apply_asset_classification(asset: dict[str, Any]) -> dict[str, Any]:
    """Mutate and return an asset dict with conservative classification fields."""
    classification = classify_asset_record(asset)
    asset.update(
        {
            "sector": classification["sector"],
            "asset_type": classification["asset_class"],
            "classification_confidence": classification["classification_confidence"],
            "classification_reason": classification["classification_reason"],
            "matched_ticker": classification["matched_ticker"],
            "is_diversified": classification["is_diversified"],
        }
    )
    if asset.get("ticker") is None and classification.get("matched_ticker"):
        asset["ticker"] = classification["matched_ticker"]
    return asset
