"""
Maps Congress.gov policy areas, legislative subjects, and stock tickers to GICS sectors.
This is the analytical linchpin: without it no rule can connect a bill to a holding.

Sectors align with GICS (Global Industry Classification Standard):
  energy, materials, industrials, consumer_discretionary, consumer_staples,
  health_care, financials, information_technology, communication_services,
  utilities, real_estate, defense
"""

# Congress.gov policy area → GICS sector(s)
POLICY_AREA_TO_SECTORS: dict[str, list[str]] = {
    "Agriculture and Food": ["consumer_staples", "materials"],
    "Animals": ["consumer_staples"],
    "Armed Forces and National Security": ["defense", "industrials"],
    "Arts, Culture, Religion": [],
    "Civil Rights and Liberties, Minority Issues": [],
    "Commerce": ["financials", "industrials", "consumer_discretionary"],
    "Congress": [],
    "Crime and Law Enforcement": [],
    "Economics and Public Finance": ["financials"],
    "Education": [],
    "Emergency Management": [],
    "Energy": ["energy"],
    "Environmental Protection": ["utilities", "materials", "energy"],
    "Families": [],
    "Finance and Financial Sector": ["financials"],
    "Foreign Trade and International Finance": ["financials", "industrials"],
    "Government Operations and Politics": [],
    "Health": ["health_care"],
    "Housing and Community Development": ["real_estate", "financials"],
    "Immigration": [],
    "International Affairs": [],
    "Labor and Employment": [],
    "Law": [],
    "Native Americans": [],
    "Private Legislation": [],
    "Public Lands and Natural Resources": ["materials", "energy", "real_estate"],
    "Science, Technology, Communications": ["information_technology", "communication_services"],
    "Social Sciences and History": [],
    "Social Welfare": ["health_care"],
    "Sports and Recreation": ["consumer_discretionary"],
    "Taxation": ["financials"],
    "Transportation and Public Works": ["industrials", "utilities"],
    "Water Resources Development": ["utilities", "materials"],
}

# Common legislative subject keywords → sectors
SUBJECT_KEYWORD_TO_SECTOR: dict[str, str] = {
    "bank": "financials",
    "banking": "financials",
    "securities": "financials",
    "insurance": "financials",
    "credit": "financials",
    "mortgage": "financials",
    "derivative": "financials",
    "hedge fund": "financials",
    "private equity": "financials",
    "oil": "energy",
    "gas": "energy",
    "petroleum": "energy",
    "coal": "energy",
    "renewable energy": "energy",
    "solar": "energy",
    "wind energy": "energy",
    "nuclear": "energy",
    "pipeline": "energy",
    "pharmaceutical": "health_care",
    "drug": "health_care",
    "medicare": "health_care",
    "medicaid": "health_care",
    "hospital": "health_care",
    "health insurance": "health_care",
    "biotechnology": "health_care",
    "medical device": "health_care",
    "technology": "information_technology",
    "cybersecurity": "information_technology",
    "semiconductor": "information_technology",
    "artificial intelligence": "information_technology",
    "software": "information_technology",
    "broadband": "communication_services",
    "telecommunications": "communication_services",
    "internet": "communication_services",
    "social media": "communication_services",
    "broadcasting": "communication_services",
    "media": "communication_services",
    "defense": "defense",
    "military": "defense",
    "weapon": "defense",
    "contractor": "defense",
    "aerospace": "defense",
    "airline": "industrials",
    "aviation": "industrials",
    "railroad": "industrials",
    "shipping": "industrials",
    "manufacturing": "industrials",
    "infrastructure": "industrials",
    "real estate": "real_estate",
    "housing": "real_estate",
    "mortgage": "real_estate",
    "agriculture": "consumer_staples",
    "food": "consumer_staples",
    "retail": "consumer_discretionary",
    "automobile": "consumer_discretionary",
    "gaming": "consumer_discretionary",
    "mining": "materials",
    "chemical": "materials",
    "steel": "materials",
    "timber": "materials",
    "water": "utilities",
    "electricity": "utilities",
    "utility": "utilities",
    "electric grid": "utilities",
}

# GICS sector codes for common stock tickers (spot-check list; expanded at runtime if needed)
TICKER_TO_SECTOR: dict[str, str] = {
    # Energy
    "XOM": "energy", "CVX": "energy", "COP": "energy", "SLB": "energy",
    "OXY": "energy", "PSX": "energy", "VLO": "energy", "HAL": "energy",
    "MPC": "energy", "DVN": "energy", "PXD": "energy", "EOG": "energy",
    # Financials
    "JPM": "financials", "BAC": "financials", "WFC": "financials", "C": "financials",
    "GS": "financials", "MS": "financials", "BLK": "financials", "AXP": "financials",
    "USB": "financials", "PNC": "financials", "TFC": "financials", "COF": "financials",
    "SCHW": "financials", "CME": "financials", "ICE": "financials", "V": "financials",
    "MA": "financials", "DFS": "financials", "BRK.B": "financials", "PRU": "financials",
    "MET": "financials", "AFL": "financials", "ALL": "financials",
    # Health Care
    "JNJ": "health_care", "UNH": "health_care", "PFE": "health_care", "ABT": "health_care",
    "MRK": "health_care", "ABBV": "health_care", "LLY": "health_care", "BMY": "health_care",
    "AMGN": "health_care", "MDT": "health_care", "CVS": "health_care", "CI": "health_care",
    "HUM": "health_care", "GILD": "health_care", "BIIB": "health_care", "REGN": "health_care",
    "ISRG": "health_care", "EW": "health_care", "TMO": "health_care", "DHR": "health_care",
    # Information Technology
    "AAPL": "information_technology", "MSFT": "information_technology",
    "NVDA": "information_technology", "AVGO": "information_technology",
    "ORCL": "information_technology", "CRM": "information_technology",
    "ADBE": "information_technology", "AMD": "information_technology",
    "INTC": "information_technology", "QCOM": "information_technology",
    "TXN": "information_technology", "IBM": "information_technology",
    "ACN": "information_technology", "CSCO": "information_technology",
    "NOW": "information_technology", "INTU": "information_technology",
    # Communication Services
    "GOOGL": "communication_services", "GOOG": "communication_services",
    "META": "communication_services", "NFLX": "communication_services",
    "DIS": "communication_services", "CMCSA": "communication_services",
    "T": "communication_services", "VZ": "communication_services",
    "TMUS": "communication_services", "CHTR": "communication_services",
    "SNAP": "communication_services", "TWTR": "communication_services",
    # Consumer Discretionary
    "AMZN": "consumer_discretionary", "TSLA": "consumer_discretionary",
    "HD": "consumer_discretionary", "MCD": "consumer_discretionary",
    "NKE": "consumer_discretionary", "SBUX": "consumer_discretionary",
    "LOW": "consumer_discretionary", "TGT": "consumer_discretionary",
    "GM": "consumer_discretionary", "F": "consumer_discretionary",
    "BKNG": "consumer_discretionary", "MAR": "consumer_discretionary",
    "EBAY": "consumer_discretionary", "ETSY": "consumer_discretionary",
    # Consumer Staples
    "PG": "consumer_staples", "KO": "consumer_staples", "PEP": "consumer_staples",
    "WMT": "consumer_staples", "COST": "consumer_staples", "PM": "consumer_staples",
    "MO": "consumer_staples", "CL": "consumer_staples", "GIS": "consumer_staples",
    "K": "consumer_staples", "HRL": "consumer_staples", "CAG": "consumer_staples",
    # Industrials
    "HON": "industrials", "GE": "industrials", "CAT": "industrials", "BA": "industrials",
    "MMM": "industrials", "UPS": "industrials", "FDX": "industrials", "RTX": "industrials",
    "LMT": "defense", "NOC": "defense", "GD": "defense", "LHX": "defense",
    "TDG": "industrials", "EMR": "industrials", "ETN": "industrials",
    # Materials
    "LIN": "materials", "APD": "materials", "ECL": "materials", "NEM": "materials",
    "FCX": "materials", "NUE": "materials", "ALB": "materials", "CF": "materials",
    # Real Estate
    "AMT": "real_estate", "PLD": "real_estate", "CCI": "real_estate", "EQIX": "real_estate",
    "SPG": "real_estate", "O": "real_estate", "PSA": "real_estate", "WELL": "real_estate",
    # Utilities
    "NEE": "utilities", "DUK": "utilities", "SO": "utilities", "D": "utilities",
    "AEP": "utilities", "EXC": "utilities", "XEL": "utilities", "SRE": "utilities",
    # Broad market / index funds → marked as diversified (discount applied in scorer)
    "SPY": "diversified", "QQQ": "diversified", "IVV": "diversified", "VTI": "diversified",
    "VOO": "diversified", "VEA": "diversified", "AGG": "diversified", "BND": "diversified",
}

# Committee code → regulated sectors (used by committee_donor rule)
COMMITTEE_TO_SECTORS: dict[str, list[str]] = {
    # Senate committees
    "SSAF": ["defense", "industrials"],                    # Armed Services
    "SSFI": ["financials"],                                # Finance
    "SSBK": ["financials", "real_estate"],                # Banking, Housing, Urban Affairs
    "SSEG": ["energy", "utilities", "materials"],          # Energy and Natural Resources
    "SSEV": ["energy", "utilities", "materials"],          # Environment and Public Works
    "SSHR": ["health_care"],                               # Health, Education, Labor
    "SSCM": ["communication_services", "information_technology"],  # Commerce
    "SSJU": [],                                            # Judiciary
    "SSFR": [],                                            # Foreign Relations
    "SSAP": [],                                            # Appropriations
    "SSBU": ["financials"],                                # Budget
    "SSGA": [],                                            # Homeland Security
    "SSVA": ["health_care"],                               # Veterans Affairs
    "SSAG": ["consumer_staples", "materials"],             # Agriculture
    "SSRN": ["consumer_staples", "materials"],             # Rules and Administration
    "SSSI": ["energy", "real_estate", "materials"],        # Indian Affairs / Small Business

    # House committees
    "HSAP": [],                                            # Appropriations
    "HSAS": ["defense", "industrials"],                    # Armed Services
    "HSBA": ["financials", "real_estate"],                # Financial Services
    "HSCM": ["communication_services", "information_technology", "energy"],  # Energy and Commerce
    "HSED": ["health_care"],                               # Education and Labor
    "HSFA": [],                                            # Foreign Affairs
    "HSHM": [],                                            # Homeland Security
    "HSJU": [],                                            # Judiciary
    "HSNR": ["energy", "materials", "real_estate"],        # Natural Resources
    "HSOG": [],                                            # Oversight and Reform
    "HSRU": [],                                            # Rules
    "HSSM": ["consumer_discretionary", "industrials"],    # Small Business
    "HSSCI": ["defense", "information_technology"],        # Intelligence
    "HSTB": ["industrials", "utilities"],                  # Transportation and Infrastructure
    "HSVR": ["health_care"],                               # Veterans Affairs
    "HSWM": ["financials", "health_care"],                 # Ways and Means
    "HSAG": ["consumer_staples", "materials"],             # Agriculture
    "HSBU": ["financials"],                                # Budget
    "HSO": [],                                             # Administration
}

# Common mutual fund / ETF name patterns → diversified flag
DIVERSIFIED_PATTERNS = [
    "index fund", "etf", "s&p 500", "total market", "bond fund",
    "treasury", "money market", "target date", "vanguard", "fidelity",
    "iShares", "schwab", "spdr", "vwiax", "vtsax", "fxaix",
]


def policy_area_to_sectors(policy_area: str) -> list[str]:
    return POLICY_AREA_TO_SECTORS.get(policy_area, [])


def subjects_to_sectors(subjects: list[str]) -> list[str]:
    sectors = set()
    for subject in subjects:
        lower = subject.lower()
        for keyword, sector in SUBJECT_KEYWORD_TO_SECTOR.items():
            if keyword in lower:
                sectors.add(sector)
    return list(sectors)


def ticker_to_sector(ticker: str) -> str:
    return TICKER_TO_SECTOR.get(ticker.upper(), "unknown")


def asset_name_is_diversified(name: str) -> bool:
    lower = name.lower()
    return any(p in lower for p in DIVERSIFIED_PATTERNS)


def bill_sectors(policy_area: str, subjects: list[str]) -> list[str]:
    """Return all sectors a bill touches based on its policy area and subjects."""
    sectors = set(policy_area_to_sectors(policy_area))
    sectors.update(subjects_to_sectors(subjects))
    return list(sectors)


def committee_sectors(committee_code: str) -> list[str]:
    return COMMITTEE_TO_SECTORS.get(committee_code, [])
