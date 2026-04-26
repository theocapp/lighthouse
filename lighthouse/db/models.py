import os
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean, Column, Date, DateTime, Float, ForeignKey,
    Index, Integer, MetaData, Numeric, String, Text, create_engine, inspect, text,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker


CORE_SCHEMA = os.environ.get("LIGHTHOUSE_DB_CORE_SCHEMA", "core")
RAW_SCHEMA = os.environ.get("LIGHTHOUSE_DB_RAW_SCHEMA", "raw")
ANALYTICS_SCHEMA = os.environ.get("LIGHTHOUSE_DB_ANALYTICS_SCHEMA", "analytics")


class Base(DeclarativeBase):
    metadata = MetaData(schema=CORE_SCHEMA)


class Member(Base):
    __tablename__ = "members"

    bioguide_id = Column(String(10), primary_key=True)
    full_name = Column(String(200), nullable=False)
    first_name = Column(String(100))
    last_name = Column(String(100))
    party = Column(String(5))          # D, R, I
    state = Column(String(2))
    district = Column(Integer)         # NULL for senators
    chamber = Column(String(10), nullable=False)   # house | senate
    is_active = Column(Boolean, default=True)
    fec_candidate_id = Column(String(20))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    committee_memberships = relationship("CommitteeMembership", back_populates="member")
    identifiers = relationship("MemberIdentifier", back_populates="member")
    sponsored_bills = relationship("Bill", back_populates="sponsor")
    votes = relationship("MemberVote", back_populates="member")
    disclosures = relationship("FinancialDisclosure", back_populates="member")
    transactions = relationship("StockTransaction", back_populates="member")
    contributions = relationship("CampaignContribution", back_populates="member")
    conflicts = relationship("Conflict", back_populates="member")


class MemberIdentifier(Base):
    __tablename__ = "member_identifiers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bioguide_id = Column(String(10), ForeignKey("members.bioguide_id"), nullable=False)
    identifier_type = Column(String(40), nullable=False)
    identifier_value = Column(String(100), nullable=False)
    is_primary = Column(Boolean, default=False)
    source = Column(String(30), default="manual")
    created_at = Column(DateTime, default=datetime.utcnow)

    member = relationship("Member", back_populates="identifiers")


class RawBillStatusFile(Base):
    __tablename__ = "billstatus_files"
    __table_args__ = {"schema": RAW_SCHEMA}

    bill_id = Column(String(50), primary_key=True)
    congress = Column(Integer, nullable=False)
    bill_type = Column(String(10), nullable=False)
    bill_number = Column(Integer)
    source_path = Column(Text)
    source_url = Column(Text)
    file_name = Column(String(255))
    xml_sha256 = Column(String(64), nullable=False)
    xml_content = Column(Text, nullable=False)
    imported_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class RawVoteFile(Base):
    __tablename__ = "vote_files"
    __table_args__ = {"schema": RAW_SCHEMA}

    vote_id = Column(String(50), primary_key=True)
    chamber = Column(String(10), nullable=False)
    congress = Column(Integer, nullable=False)
    session = Column(Integer, nullable=False)
    vote_number = Column(Integer, nullable=False)
    source_url = Column(Text)
    source_format = Column(String(20), nullable=False)
    content_sha256 = Column(String(64), nullable=False)
    raw_content = Column(Text, nullable=False)
    imported_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class RawFecCommittee(Base):
    __tablename__ = "fec_committees"
    __table_args__ = {"schema": RAW_SCHEMA}

    committee_id = Column(String(9), primary_key=True)
    file_year = Column(Integer, primary_key=True)
    committee_name = Column(String(200))
    committee_designation = Column(String(1))
    committee_type = Column(String(1))
    committee_party = Column(String(10))
    organization_type = Column(String(1))
    connected_org_name = Column(String(200))
    candidate_id = Column(String(9))
    imported_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class RawFecCandidateCommitteeLinkage(Base):
    __tablename__ = "fec_candidate_committee_linkages"
    __table_args__ = {"schema": RAW_SCHEMA}

    candidate_id = Column(String(9), primary_key=True)
    committee_id = Column(String(9), primary_key=True)
    file_year = Column(Integer, primary_key=True)
    candidate_election_year = Column(Integer)
    fec_election_year = Column(Integer)
    committee_type = Column(String(1))
    committee_designation = Column(String(1))
    linkage_id = Column(String(20))
    committee_name = Column(String(200))
    member_bioguide_id = Column(String(10))
    imported_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class RawFecIndividualContribution(Base):
    __tablename__ = "fec_individual_contributions"
    __table_args__ = {"schema": RAW_SCHEMA}

    source_sub_id = Column(String(100), primary_key=True)
    file_year = Column(Integer, primary_key=True)
    member_bioguide_id = Column(String(10), nullable=False)
    candidate_id = Column(String(9))
    committee_id = Column(String(9), nullable=False)
    committee_name = Column(String(200))
    contributor_name = Column(Text)
    contributor_employer = Column(Text)
    contributor_occupation = Column(Text)
    contributor_state = Column(String(2))
    entity_type = Column(String(3))
    transaction_type = Column(String(3))
    transaction_dt_raw = Column(String(100))
    transaction_date = Column(Date)
    amount = Column(Numeric(14, 2))
    other_id = Column(String(9))
    image_num = Column(String(20))
    memo_code = Column(String(5))
    memo_text = Column(Text)
    source_transaction_id = Column(String(100))
    derived_sector = Column(String(50))
    source_record_hash = Column(String(64))
    imported_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CommitteeMembership(Base):
    __tablename__ = "committee_memberships"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bioguide_id = Column(String(10), ForeignKey("members.bioguide_id"), nullable=False)
    committee_code = Column(String(20), nullable=False)
    committee_name = Column(String(300))
    role = Column(String(20))          # member | chair | ranking
    congress = Column(Integer)
    start_date = Column(Date)
    end_date = Column(Date)

    member = relationship("Member", back_populates="committee_memberships")


class Bill(Base):
    __tablename__ = "bills"

    bill_id = Column(String(50), primary_key=True)   # e.g. "hr1234-119"
    bill_number = Column(Integer)
    bill_type = Column(String(10))     # hr, s, hres, etc.
    congress = Column(Integer)
    title = Column(Text)
    short_title = Column(Text)
    introduced_date = Column(Date)
    status = Column(Text)
    policy_area = Column(String(200))
    sponsor_bioguide = Column(String(10), ForeignKey("members.bioguide_id"))
    subjects_json = Column(Text)       # JSON array of legislative subjects
    industries_json = Column(Text)     # JSON array of derived sector tags
    govinfo_url = Column(Text)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    sponsor = relationship("Member", back_populates="sponsored_bills")
    cosponsors = relationship("BillCosponsor", back_populates="bill")
    votes = relationship("Vote", back_populates="bill")
    conflicts = relationship("Conflict", back_populates="bill")


class BillCosponsor(Base):
    __tablename__ = "bill_cosponsors"

    bill_id = Column(String(50), ForeignKey("bills.bill_id"), primary_key=True)
    bioguide_id = Column(String(10), ForeignKey("members.bioguide_id"), primary_key=True)
    cosponsor_date = Column(Date)

    bill = relationship("Bill", back_populates="cosponsors")
    member = relationship("Member")


class Vote(Base):
    __tablename__ = "votes"

    vote_id = Column(String(50), primary_key=True)   # e.g. "s2025-042"
    chamber = Column(String(10))
    congress = Column(Integer)
    session = Column(Integer)
    vote_number = Column(Integer)
    vote_date = Column(DateTime)
    question = Column(Text)
    result = Column(String(100))
    bill_id = Column(String(50), ForeignKey("bills.bill_id"))
    category = Column(String(50))      # passage | amendment | cloture
    requires = Column(String(10))      # 1/2 | 2/3
    source_url = Column(Text)

    bill = relationship("Bill", back_populates="votes")
    member_votes = relationship("MemberVote", back_populates="vote")
    conflicts = relationship("Conflict", back_populates="vote")


class MemberVote(Base):
    __tablename__ = "member_votes"

    vote_id = Column(String(50), ForeignKey("votes.vote_id"), primary_key=True)
    bioguide_id = Column(String(10), ForeignKey("members.bioguide_id"), primary_key=True)
    position = Column(String(20), nullable=False)  # Yea | Nay | Not Voting | Present

    vote = relationship("Vote", back_populates="member_votes")
    member = relationship("Member", back_populates="votes")


class FinancialDisclosure(Base):
    __tablename__ = "financial_disclosures"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bioguide_id = Column(String(10), ForeignKey("members.bioguide_id"), nullable=False)
    filer_name = Column(String(200))
    filer_type = Column(String(20))    # member | spouse | dependent
    filing_type = Column(String(30))   # annual | amendment | new_filer
    year = Column(Integer)
    filed_date = Column(Date)
    source = Column(String(10))        # house | senate
    source_url = Column(Text)
    raw_file_path = Column(Text)
    parsed_at = Column(DateTime)

    member = relationship("Member", back_populates="disclosures")
    assets = relationship("Asset", back_populates="disclosure")


class Asset(Base):
    __tablename__ = "assets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    disclosure_id = Column(Integer, ForeignKey("financial_disclosures.id"), nullable=False)
    bioguide_id = Column(String(10), ForeignKey("members.bioguide_id"), nullable=False)
    asset_name = Column(Text, nullable=False)
    asset_type = Column(String(30))    # public_equity | bond | fund | real_estate | private_business | trust | unknown
    ticker = Column(String(20))
    value_min = Column(Numeric(15, 2))
    value_max = Column(Numeric(15, 2))
    income_min = Column(Numeric(15, 2))
    income_max = Column(Numeric(15, 2))
    owner = Column(String(20))         # self | spouse | joint | dependent
    year = Column(Integer)
    industry_code = Column(String(20))
    sector = Column(String(50))

    disclosure = relationship("FinancialDisclosure", back_populates="assets")
    conflicts = relationship("Conflict", back_populates="asset")


class StockTransaction(Base):
    __tablename__ = "stock_transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bioguide_id = Column(String(10), ForeignKey("members.bioguide_id"), nullable=False)
    transaction_date = Column(Date, nullable=False)
    disclosure_date = Column(Date)
    ticker = Column(String(20))
    asset_name = Column(Text)
    transaction_type = Column(String(20))  # purchase | sale | sale_partial
    amount_min = Column(Numeric(15, 2))
    amount_max = Column(Numeric(15, 2))
    owner = Column(String(20))             # self | spouse | joint
    source = Column(String(30))            # house_watcher | senate_watcher | disclosure
    comment = Column(Text)
    source_url = Column(Text)
    source_file = Column(Text)
    source_key = Column(String(200))
    source_hash = Column(String(64))
    sector = Column(String(50))
    industry_code = Column(String(20))

    member = relationship("Member", back_populates="transactions")
    conflicts = relationship("Conflict", back_populates="transaction")


class CampaignContribution(Base):
    __tablename__ = "campaign_contributions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bioguide_id = Column(String(10), ForeignKey("members.bioguide_id"), nullable=False)
    fec_committee_id = Column(String(20))
    contributor_name = Column(Text)
    contributor_employer = Column(Text)
    contributor_industry = Column(String(100))
    amount = Column(Numeric(12, 2))
    contribution_date = Column(Date)
    election_cycle = Column(Integer)
    contribution_type = Column(String(20))  # individual | pac
    source_table = Column(String(100))
    source_key = Column(String(200))
    source_url = Column(Text)
    source_file = Column(Text)
    source_hash = Column(String(64))
    source_sub_id = Column(String(100))
    source_image_num = Column(String(20))
    source_transaction_id = Column(String(100))

    member = relationship("Member", back_populates="contributions")
    conflicts = relationship("Conflict", back_populates="contribution")


class IngestionLog(Base):
    """Tracks last successful ingestion timestamp per source for incremental updates."""
    __tablename__ = "ingestion_log"

    source = Column(String(50), primary_key=True)
    last_run = Column(DateTime)
    records_added = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    status = Column(String(20))   # ok | error
    error_message = Column(Text)


class Conflict(Base):
    __tablename__ = "conflicts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bioguide_id = Column(String(10), ForeignKey("members.bioguide_id"), nullable=False)
    conflict_type = Column(String(40), nullable=False)
    score = Column(Float, nullable=False)
    confidence = Column(String(10))    # high | medium | low

    vote_id = Column(String(50), ForeignKey("votes.vote_id"))
    bill_id = Column(String(50), ForeignKey("bills.bill_id"))
    asset_id = Column(Integer, ForeignKey("assets.id"))
    transaction_id = Column(Integer, ForeignKey("stock_transactions.id"))
    contribution_id = Column(Integer, ForeignKey("campaign_contributions.id"))

    evidence_summary = Column(Text)
    detail_json = Column(Text)         # full evidence payload as JSON

    detected_at = Column(DateTime, default=datetime.utcnow)
    is_reviewed = Column(Boolean, default=False)
    review_note = Column(Text)

    member = relationship("Member", back_populates="conflicts")
    vote = relationship("Vote", back_populates="conflicts")
    bill = relationship("Bill", back_populates="conflicts")
    asset = relationship("Asset", back_populates="conflicts")
    transaction = relationship("StockTransaction", back_populates="conflicts")
    contribution = relationship("CampaignContribution", back_populates="conflicts")


class ElectionRace(Base):
    __tablename__ = "election_races"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cycle = Column(Integer, nullable=False)
    state = Column(String(2), nullable=False)
    office = Column(String(100))
    office_level = Column(String(20))       # federal | state | local
    district = Column(String(20))
    stage = Column(String(20))              # general | primary | runoff
    special = Column(Boolean, default=False)
    election_date = Column(Date, nullable=True)
    total_votes = Column(Integer, nullable=True)
    source = Column(String(50))             # mit_election_lab | manual
    source_key = Column(String(200), unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    candidates = relationship("ElectionCandidate", back_populates="race", cascade="all, delete-orphan")


class ElectionCandidate(Base):
    __tablename__ = "election_candidates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    race_id = Column(Integer, ForeignKey("election_races.id"), nullable=False)
    bioguide_id = Column(String(10), ForeignKey("members.bioguide_id"), nullable=True)
    candidate_name = Column(String(200), nullable=False)
    party = Column(String(50))
    votes = Column(Integer, nullable=True)
    vote_pct = Column(Float, nullable=True)
    winner = Column(Boolean, default=False)
    incumbent = Column(Boolean, default=False)
    fec_candidate_id = Column(String(20), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    race = relationship("ElectionRace", back_populates="candidates")
    member = relationship("Member")


# Indexes for common query patterns
Index("idx_election_races_state_cycle", ElectionRace.state, ElectionRace.cycle)
Index("idx_election_candidates_bioguide", ElectionCandidate.bioguide_id)
Index("idx_conflicts_member", Conflict.bioguide_id)
Index("idx_conflicts_type", Conflict.conflict_type)
Index("idx_conflicts_score", Conflict.score)
Index("idx_raw_billstatus_congress_type_number", RawBillStatusFile.congress, RawBillStatusFile.bill_type, RawBillStatusFile.bill_number)
Index("idx_raw_vote_chamber_congress_session_number", RawVoteFile.chamber, RawVoteFile.congress, RawVoteFile.session, RawVoteFile.vote_number)
Index("idx_raw_fec_committees_candidate_year", RawFecCommittee.candidate_id, RawFecCommittee.file_year)
Index("idx_raw_fec_linkages_member_year", RawFecCandidateCommitteeLinkage.member_bioguide_id, RawFecCandidateCommitteeLinkage.file_year)
Index("idx_raw_fec_contrib_member_year", RawFecIndividualContribution.member_bioguide_id, RawFecIndividualContribution.file_year)
Index("idx_raw_fec_contrib_committee_year", RawFecIndividualContribution.committee_id, RawFecIndividualContribution.file_year)
Index("idx_raw_fec_contrib_source_txn_id", RawFecIndividualContribution.source_transaction_id)
Index("idx_member_identifiers_type_value", MemberIdentifier.identifier_type, MemberIdentifier.identifier_value)
Index("idx_member_identifiers_bioguide", MemberIdentifier.bioguide_id)
Index("idx_stock_transactions_member_date", StockTransaction.bioguide_id, StockTransaction.transaction_date)
Index("idx_stock_transactions_source_key", StockTransaction.source_key)
Index("idx_stock_transactions_source_hash", StockTransaction.source_hash)
Index("idx_member_votes_member", MemberVote.bioguide_id)
Index("idx_assets_member_ticker", Asset.bioguide_id, Asset.ticker)
Index("idx_bills_sponsor", Bill.sponsor_bioguide)
Index("idx_bills_policy_area", Bill.policy_area)
Index("idx_campaign_contributions_source_key", CampaignContribution.source_key)
Index("idx_campaign_contributions_source_sub_id", CampaignContribution.source_sub_id)
Index("idx_campaign_contributions_source_hash", CampaignContribution.source_hash)


def get_engine(db_url: str):
    return create_engine(db_url, echo=False)


def get_session(db_url: str) -> Session:
    engine = get_engine(db_url)
    init_db(db_url, engine=engine)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def init_db(db_url: str, engine=None):
    engine = engine or get_engine(db_url)
    _ensure_schemas(engine, db_url)
    Base.metadata.create_all(engine)
    upgrade_db(engine)
    return engine


def upgrade_db(engine) -> None:
    inspector = inspect(engine)
    core_schema = CORE_SCHEMA if engine.dialect.name == "postgresql" else None
    raw_schema = RAW_SCHEMA if engine.dialect.name == "postgresql" else None
    existing_tables = set(inspector.get_table_names(schema=core_schema))
    raw_tables = set(inspector.get_table_names(schema=raw_schema))

    if "campaign_contributions" in existing_tables:
        _ensure_columns(
            engine,
            schema=core_schema,
            table_name="campaign_contributions",
            columns={
                "source_table": "TEXT",
                "source_key": "VARCHAR(200)",
                "source_url": "TEXT",
                "source_file": "TEXT",
                "source_hash": "VARCHAR(64)",
                "source_sub_id": "VARCHAR(100)",
                "source_image_num": "VARCHAR(20)",
                "source_transaction_id": "VARCHAR(100)",
            },
        )
        _ensure_index(engine, schema=core_schema, table_name="campaign_contributions", index_name="idx_campaign_contributions_source_key", columns=["source_key"])
        _ensure_index(engine, schema=core_schema, table_name="campaign_contributions", index_name="idx_campaign_contributions_source_sub_id", columns=["source_sub_id"])
        _ensure_index(engine, schema=core_schema, table_name="campaign_contributions", index_name="idx_campaign_contributions_source_hash", columns=["source_hash"])

    if "stock_transactions" in existing_tables:
        _ensure_columns(
            engine,
            schema=core_schema,
            table_name="stock_transactions",
            columns={
                "source_url": "TEXT",
                "source_file": "TEXT",
                "source_key": "VARCHAR(200)",
                "source_hash": "VARCHAR(64)",
            },
        )
        _ensure_index(engine, schema=core_schema, table_name="stock_transactions", index_name="idx_stock_transactions_source_key", columns=["source_key"])
        _ensure_index(engine, schema=core_schema, table_name="stock_transactions", index_name="idx_stock_transactions_source_hash", columns=["source_hash"])

    raw_table_exists = "fec_individual_contributions" in raw_tables
    if raw_table_exists:
        _ensure_columns(
            engine,
            schema=raw_schema,
            table_name="fec_individual_contributions",
            columns={
                "memo_code": "VARCHAR(5)",
                "source_transaction_id": "VARCHAR(100)",
            },
        )
        _ensure_index(engine, schema=raw_schema, table_name="fec_individual_contributions", index_name="idx_raw_fec_contrib_source_txn_id", columns=["source_transaction_id"])


def _ensure_schemas(engine, db_url: str):
    if not db_url.startswith("postgresql"):
        return

    with engine.begin() as conn:
        for schema in (RAW_SCHEMA, CORE_SCHEMA, ANALYTICS_SCHEMA):
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))


def _ensure_columns(engine, *, schema: Optional[str], table_name: str, columns: dict[str, str]) -> None:
    inspector = inspect(engine)
    existing = {col["name"] for col in inspector.get_columns(table_name, schema=schema)}
    with engine.begin() as conn:
        for column_name, column_type in columns.items():
            if column_name in existing:
                continue
            qualified = _qualified_name(engine.dialect.name, schema, table_name)
            conn.execute(text(f"ALTER TABLE {qualified} ADD COLUMN {column_name} {column_type}"))


def _ensure_index(engine, *, schema: Optional[str], table_name: str, index_name: str, columns: list[str]) -> None:
    inspector = inspect(engine)
    existing = {idx["name"] for idx in inspector.get_indexes(table_name, schema=schema)}
    if index_name in existing:
        return
    qualified = _qualified_name(engine.dialect.name, schema, table_name)
    column_sql = ", ".join(columns)
    if engine.dialect.name == "postgresql":
        sql = f'CREATE INDEX IF NOT EXISTS "{index_name}" ON {qualified} ({column_sql})'
    else:
        sql = f'CREATE INDEX IF NOT EXISTS "{index_name}" ON {qualified} ({column_sql})'
    with engine.begin() as conn:
        conn.execute(text(sql))


def _qualified_name(dialect_name: str, schema: Optional[str], table_name: str) -> str:
    if dialect_name == "postgresql" and schema:
        return f'"{schema}"."{table_name}"'
    return table_name
