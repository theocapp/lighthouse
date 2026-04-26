from types import SimpleNamespace

from lighthouse.config import Config
from lighthouse.db.models import (
    CampaignContribution,
    Member,
    RawFecCandidateCommitteeLinkage,
    RawFecCommittee,
    RawFecIndividualContribution,
    StockTransaction,
)
from lighthouse.collectors.fec import normalize_contribution
from lighthouse.collectors.house_stocks import normalize_house_transaction
from lighthouse.pipeline import ingest as ingest_mod
from lighthouse.pipeline.ingest import (
    IngestPipeline,
    _financial_disclosure_payload,
    _is_later_local_fec_row,
    _is_memo_only_local_fec_row,
    _local_fec_row_dedupe_key,
    _stock_transaction_dedup_key,
)


class _QueryStub:
    def __init__(self, session, model):
        self.session = session
        self.model = model

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        if self.model is Member:
            return self.session.members
        return []

    def delete(self, synchronize_session=False):
        self.session.deleted_models.append(self.model)
        self.session.persisted = [
            obj for obj in self.session.persisted if not isinstance(obj, self.model)
        ]
        return 0


class _SessionStub:
    def __init__(self, members):
        self.members = members
        self.deleted_models = []
        self.added = []
        self.persisted = []
        self.objects = {}
        self.flushed = False

    def query(self, model):
        return _QueryStub(self, model)

    def get(self, model, key):
        return self.objects.get((model, key))

    def add(self, obj):
        self.added.append(obj)
        self.persisted.append(obj)
        key = _model_key(obj)
        if key is not None:
            self.objects[(obj.__class__, key)] = obj

    def flush(self):
        self.flushed = True


def _model_key(obj):
    if isinstance(obj, RawFecCommittee):
        return (obj.committee_id, obj.file_year)
    if isinstance(obj, RawFecCandidateCommitteeLinkage):
        return (obj.candidate_id, obj.committee_id, obj.file_year)
    if isinstance(obj, RawFecIndividualContribution):
        return (obj.source_sub_id, obj.file_year)
    return None


def test_fec_api_ingestion_deletes_current_cycle_and_skips_duplicates(monkeypatch, tmp_path):
    class FakeFecCollector:
        def __init__(self, api_key, cache_dir, rate):
            pass

        def get_candidate_committees(self, candidate_id, cycle):
            return [{"committee_id": "CMT1"}]

        def get_contributions_to_committee(self, committee_id, cycle):
            row = {
                "committee_id": committee_id,
                "contributor_name": "Jane Donor",
                "contributor_employer": "Acme",
                "contributor_industry": "energy",
                "contribution_receipt_amount": 250.0,
                "contribution_receipt_date": "2024-05-10",
                "two_year_transaction_period": cycle,
                "entity_type": "IND",
            }
            yield row
            yield dict(row)

    monkeypatch.setattr(ingest_mod, "FecCollector", FakeFecCollector)
    monkeypatch.setattr(ingest_mod.q, "get_member_fec_ids", lambda session, bioguide_id: ["H0XX"])

    cfg = Config()
    cfg.api_keys.fec = "test-key"
    cfg.data.cache_dir = str(tmp_path)
    member = SimpleNamespace(bioguide_id="A000001")
    session = _SessionStub([member])
    pipeline = IngestPipeline(session, cfg)

    pipeline._ingest_fec()

    assert CampaignContribution in session.deleted_models
    contributions = [obj for obj in session.added if isinstance(obj, CampaignContribution)]
    assert len(contributions) == 1
    assert contributions[0].bioguide_id == "A000001"
    assert contributions[0].election_cycle == cfg.data.fec_cycle


def test_fec_api_normalization_preserves_available_source_identifiers():
    raw = {
        "committee_id": "CMT1",
        "contributor_name": "Jane Donor",
        "contributor_employer": "Acme",
        "contributor_industry": "energy",
        "contribution_receipt_amount": 250.0,
        "contribution_receipt_date": "2024-05-10",
        "two_year_transaction_period": 2024,
        "entity_type": "IND",
        "sub_id": "12345",
        "image_number": "IMG123",
        "transaction_id": "TX123",
    }

    normalized = normalize_contribution(raw, "A000001")

    assert normalized["source_table"] == "api.openfec.schedule_a"
    assert normalized["source_sub_id"] == "12345"
    assert normalized["source_image_num"] == "IMG123"
    assert normalized["source_transaction_id"] == "TX123"
    assert normalized["source_hash"]


def test_stock_ingestion_skips_duplicates_across_sources(monkeypatch, tmp_path):
    class FakeStockCollector:
        def __init__(self, rows):
            self.rows = rows

        def get_all_transactions(self):
            return self.rows

    class FakeHouseDisclosuresCollector:
        def __init__(self, cache_dir):
            pass

        def get_all_filings_for_year(self, year):
            return [{"doc_id": "1", "document_type": "ptr", "office": "CA12", "name": "Rep Example"}]

        def download_filing(self, filing):
            return tmp_path / "ptr.pdf"

    duplicate_row = {
        "representative": "Rep Example",
        "transaction_date": "2024-05-01",
        "disclosure_date": "2024-05-10",
        "ticker": "MSFT",
        "asset_description": "Microsoft Corp",
        "type": "purchase",
        "amount": "$15,001 - $50,000",
        "owner": "self",
        "comment": "",
    }

    def fake_parse_house_ptr_pdf(path, bioguide_id):
        return [
            {
                "bioguide_id": bioguide_id,
                "transaction_date": "2024-05-01",
                "disclosure_date": "2024-05-10",
                "ticker": "MSFT",
                "asset_name": "Microsoft Corp",
                "transaction_type": "purchase",
                "amount_min": 15001.0,
                "amount_max": 50000.0,
                "owner": "self",
                "source": "disclosure",
                "comment": None,
                "sector": None,
                "industry_code": None,
            }
        ]

    monkeypatch.setattr(ingest_mod, "HouseDisclosuresCollector", FakeHouseDisclosuresCollector)
    monkeypatch.setattr(ingest_mod, "parse_house_ptr_pdf", fake_parse_house_ptr_pdf)

    cfg = Config()
    cfg.data.cache_dir = str(tmp_path)
    member = SimpleNamespace(
        bioguide_id="A000001",
        chamber="house",
        state="CA",
        district=12,
        full_name="Rep Example",
        first_name="Rep",
        last_name="Example",
    )
    session = _SessionStub([member])
    pipeline = IngestPipeline(session, cfg)
    pipeline._house_stocks = FakeStockCollector([duplicate_row])
    pipeline._senate_stocks = FakeStockCollector([])

    pipeline._ingest_stocks()

    assert StockTransaction in session.deleted_models
    trades = [obj for obj in session.added if isinstance(obj, StockTransaction)]
    assert len(trades) == 1
    assert trades[0].ticker == "MSFT"
    assert trades[0].source_file


def test_financial_disclosure_payload_preserves_senate_source_provenance():
    member = SimpleNamespace(bioguide_id="S000001")
    filing = {
        "first_name": "Jane",
        "last_name": "Senator",
        "report_type": "Annual",
        "filed_date": "05/12/2024",
        "report_url": "https://efdsearch.senate.gov/search/view/report/123",
    }

    payload = _financial_disclosure_payload(
        member=member,
        filing=filing,
        year=2024,
        source="senate",
        raw_file_path="/tmp/report.html",
    )

    assert payload["source"] == "senate"
    assert payload["source_url"] == filing["report_url"]
    assert payload["raw_file_path"] == "/tmp/report.html"


def test_local_fec_memo_rows_are_identified_conservatively():
    assert _is_memo_only_local_fec_row({"memo_cd": "X", "transaction_tp": "15"}) is True
    assert _is_memo_only_local_fec_row({"memo_cd": None, "transaction_tp": "15J"}) is True
    assert _is_memo_only_local_fec_row({"memo_cd": None, "transaction_tp": "15E"}) is False


def test_local_fec_duplicate_rows_prefer_later_sub_id():
    older = {"sub_id": "100", "tran_id": "TX1", "cmte_id": "C1", "file_year": 2024}
    newer = {"sub_id": "101", "tran_id": "TX1", "cmte_id": "C1", "file_year": 2024}

    assert _local_fec_row_dedupe_key(older) == _local_fec_row_dedupe_key(newer)
    assert _is_later_local_fec_row(newer, older) is True


def test_local_fec_ingestion_skips_ambiguous_committees_and_preserves_raw_rows(monkeypatch, tmp_path):
    class FakeMappings(list):
        def mappings(self):
            return self

    class FakeConnection:
        def __init__(self, linkage_rows, contribution_rows):
            self.linkage_rows = linkage_rows
            self.contribution_rows = contribution_rows

        def execute(self, stmt, params):
            sql = str(stmt)
            if "candidate_committee_linkages" in sql:
                return FakeMappings(self.linkage_rows)
            return FakeMappings(self.contribution_rows)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeSourceEngine:
        def __init__(self, linkage_rows, contribution_rows):
            self.linkage_rows = linkage_rows
            self.contribution_rows = contribution_rows

        def connect(self):
            return FakeConnection(self.linkage_rows, self.contribution_rows)

    linkage_rows = [
        {
            "cand_id": "CAND1",
            "cand_election_yr": 2024,
            "fec_election_yr": 2024,
            "cmte_id": "CMT1",
            "cmte_tp": "P",
            "cmte_dsgn": "P",
            "linkage_id": "L1",
            "file_year": 2024,
            "cmte_nm": "Shared Committee",
            "org_tp": None,
            "connected_org_nm": None,
            "committee_candidate_id": "CAND1",
        },
        {
            "cand_id": "CAND2",
            "cand_election_yr": 2024,
            "fec_election_yr": 2024,
            "cmte_id": "CMT1",
            "cmte_tp": "P",
            "cmte_dsgn": "P",
            "linkage_id": "L2",
            "file_year": 2024,
            "cmte_nm": "Shared Committee",
            "org_tp": None,
            "connected_org_nm": None,
            "committee_candidate_id": "CAND2",
        },
    ]
    contribution_rows = [
        {
            "cmte_id": "CMT1",
            "name": "Jane Donor",
            "employer": "Acme",
            "occupation": "CEO",
            "state": "CA",
            "entity_tp": "IND",
            "transaction_tp": "15",
            "transaction_dt": "05012024",
            "transaction_amt": 250,
            "other_id": None,
            "image_num": "IMG1",
            "memo_text": None,
            "sub_id": "200",
            "file_year": 2024,
            "memo_cd": None,
            "tran_id": "TX1",
            "cmte_nm": "Shared Committee",
        }
    ]

    fake_engine = FakeSourceEngine(linkage_rows, contribution_rows)
    monkeypatch.setattr(ingest_mod, "create_engine", lambda *args, **kwargs: fake_engine)
    monkeypatch.setattr(ingest_mod, "_get_source_table_columns", lambda engine, table: {"memo_cd", "tran_id"})
    monkeypatch.setattr(
        ingest_mod.q,
        "get_member_fec_ids",
        lambda session, bioguide_id: ["CAND1"] if bioguide_id == "A000001" else ["CAND2"],
    )

    cfg = Config()
    cfg.fec_warehouse.source_db_url = "postgresql://fake"
    cfg.fec_warehouse.cycles = [2024]
    session = _SessionStub(
        [
            SimpleNamespace(bioguide_id="A000001"),
            SimpleNamespace(bioguide_id="B000002"),
        ]
    )
    pipeline = IngestPipeline(session, cfg)

    pipeline._ingest_fec_from_local_db()

    normalized = [obj for obj in session.persisted if isinstance(obj, CampaignContribution)]
    raw_linkages = [obj for obj in session.persisted if isinstance(obj, RawFecCandidateCommitteeLinkage)]
    assert normalized == []
    assert len(raw_linkages) == 2


def test_local_fec_ingestion_skips_memo_rows_dedupes_amendments_and_is_idempotent(monkeypatch):
    class FakeMappings(list):
        def mappings(self):
            return self

    class FakeConnection:
        def __init__(self, linkage_rows, contribution_rows):
            self.linkage_rows = linkage_rows
            self.contribution_rows = contribution_rows

        def execute(self, stmt, params):
            sql = str(stmt)
            if "candidate_committee_linkages" in sql:
                return FakeMappings(self.linkage_rows)
            return FakeMappings(self.contribution_rows)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeSourceEngine:
        def __init__(self, linkage_rows, contribution_rows):
            self.linkage_rows = linkage_rows
            self.contribution_rows = contribution_rows

        def connect(self):
            return FakeConnection(self.linkage_rows, self.contribution_rows)

    linkage_rows = [
        {
            "cand_id": "CAND1",
            "cand_election_yr": 2024,
            "fec_election_yr": 2024,
            "cmte_id": "CMT1",
            "cmte_tp": "P",
            "cmte_dsgn": "P",
            "linkage_id": "L1",
            "file_year": 2024,
            "cmte_nm": "Principal Committee",
            "org_tp": None,
            "connected_org_nm": None,
            "committee_candidate_id": "CAND1",
        }
    ]
    contribution_rows = [
        {
            "cmte_id": "CMT1",
            "name": "Jane Donor",
            "employer": "Acme",
            "occupation": "CEO",
            "state": "CA",
            "entity_tp": "IND",
            "transaction_tp": "15J",
            "transaction_dt": "05012024",
            "transaction_amt": 250,
            "other_id": None,
            "image_num": "IMG1",
            "memo_text": "memo row",
            "sub_id": "100",
            "file_year": 2024,
            "memo_cd": "X",
            "tran_id": "TX1",
            "cmte_nm": "Principal Committee",
        },
        {
            "cmte_id": "CMT1",
            "name": "Jane Donor",
            "employer": "Acme",
            "occupation": "CEO",
            "state": "CA",
            "entity_tp": "IND",
            "transaction_tp": "15",
            "transaction_dt": "05012024",
            "transaction_amt": 250,
            "other_id": None,
            "image_num": "IMG2",
            "memo_text": None,
            "sub_id": "101",
            "file_year": 2024,
            "memo_cd": None,
            "tran_id": "TX1",
            "cmte_nm": "Principal Committee",
        },
        {
            "cmte_id": "CMT1",
            "name": "Jane Donor",
            "employer": "Acme",
            "occupation": "CEO",
            "state": "CA",
            "entity_tp": "IND",
            "transaction_tp": "15",
            "transaction_dt": "05012024",
            "transaction_amt": 250,
            "other_id": None,
            "image_num": "IMG3",
            "memo_text": "amended row",
            "sub_id": "102",
            "file_year": 2024,
            "memo_cd": None,
            "tran_id": "TX1",
            "cmte_nm": "Principal Committee",
        },
    ]

    fake_engine = FakeSourceEngine(linkage_rows, contribution_rows)
    monkeypatch.setattr(ingest_mod, "create_engine", lambda *args, **kwargs: fake_engine)
    monkeypatch.setattr(ingest_mod, "_get_source_table_columns", lambda engine, table: {"memo_cd", "tran_id"})
    monkeypatch.setattr(ingest_mod.q, "get_member_fec_ids", lambda session, bioguide_id: ["CAND1"])

    cfg = Config()
    cfg.fec_warehouse.source_db_url = "postgresql://fake"
    cfg.fec_warehouse.cycles = [2024]
    session = _SessionStub([SimpleNamespace(bioguide_id="A000001")])
    pipeline = IngestPipeline(session, cfg)

    pipeline._ingest_fec_from_local_db()
    first_run = [obj for obj in session.persisted if isinstance(obj, CampaignContribution)]
    assert len(first_run) == 1
    assert first_run[0].fec_committee_id == "CMT1"
    assert first_run[0].source_table == "raw.fec_individual_contributions"
    assert first_run[0].source_sub_id == "102"
    assert first_run[0].source_transaction_id == "TX1"
    assert first_run[0].source_image_num == "IMG3"
    assert first_run[0].source_key == "102:2024"
    assert first_run[0].source_hash

    raw_rows = [obj for obj in session.persisted if isinstance(obj, RawFecIndividualContribution)]
    assert any(obj.source_sub_id == "102" for obj in raw_rows)

    pipeline._ingest_fec_from_local_db()
    second_run = [obj for obj in session.persisted if isinstance(obj, CampaignContribution)]
    assert len(second_run) == 1
    assert second_run[0].contributor_name == "Jane Donor"
    assert second_run[0].contribution_date.isoformat() == "2024-05-01"


def test_stock_transaction_provenance_is_stored_while_dedupe_still_catches_true_duplicates():
    raw = {
        "representative": "Rep Example",
        "transaction_date": "2024-05-01",
        "disclosure_date": "2024-05-10",
        "ticker": "MSFT",
        "asset_description": "Microsoft Corp",
        "type": "purchase",
        "amount": "$15,001 - $50,000",
        "owner": "self",
        "comment": "",
        "id": "watcher-1",
    }
    lookup = {"rep example": "A000001"}

    txn_a = normalize_house_transaction(raw, lookup)
    txn_b = dict(txn_a)
    txn_b["source_file"] = "/tmp/other-cache.json"
    txn_b["source_hash"] = "different"

    assert txn_a["source_url"]
    assert txn_a["source_key"]
    assert txn_a["source_hash"]
    assert _stock_transaction_dedup_key(txn_a) == _stock_transaction_dedup_key(txn_b)
