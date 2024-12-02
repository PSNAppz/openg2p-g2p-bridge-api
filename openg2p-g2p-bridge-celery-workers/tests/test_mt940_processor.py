from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from openg2p_g2p_bridge_celery_workers.tasks.mt940_processor import (
    construct_parsed_transaction,
    get_disbursement_envelope_id,
    mt940_processor_worker,
    process_debit_transactions,
    process_reversal_of_debits,
    update_envelope_batch_status_reconciled,
    update_envelope_batch_status_reversed,
)
from openg2p_g2p_bridge_models.errors.codes import G2PBridgeErrorCodes
from openg2p_g2p_bridge_models.models import (
    AccountStatement,
    AccountStatementLob,
    BenefitProgramConfiguration,
    Disbursement,
    DisbursementBatchControl,
    DisbursementEnvelopeBatchStatus,
    DisbursementRecon,
    ProcessStatus,
)


class MockSession:
    def __init__(self):
        self.committed = False
        self.flushed = False
        self.added = False
        self.account_statement = AccountStatement(
            statement_id="test_statement_id",
            account_number="test_account_number",
            statement_process_status=ProcessStatus.PENDING,
            statement_process_attempts=0,
        )
        self.account_statement_lob = AccountStatementLob(
            statement_id="test_statement_id",
            statement_lob="""
            :20:1234567890
            :25:12345678901234567890
            :28C:123/1
            :60F:C000000000000,00
            :61:2012123456789,00DTRFREF123//123456789
            BENEFICIARY/123456789
            :86:PAYMENT TO BENEFICIARY
            :62F:C000000000000,00
            """,
        )
        self.benefit_program_configuration = BenefitProgramConfiguration(
            benefit_program_mnemonic="test_program",
            sponsor_bank_code="test_bank",
            sponsor_bank_account_number="test_account_number",
        )
        self.disbursement = Disbursement(
            disbursement_id="test_disbursement_id",
            disbursement_envelope_id="test_envelope_id",
        )
        self.disbursement_envelope_batch_status = DisbursementEnvelopeBatchStatus(
            disbursement_envelope_id="test_envelope_id",
            number_of_disbursements_reconciled=0,
            number_of_disbursements_reversed=0,
        )
        self.disbursement_recon = None
        self.disbursement_batch_control = DisbursementBatchControl(
            disbursement_id="test_disbursement_id",
            bank_disbursement_batch_id="test_batch_id",
            mapper_status=ProcessStatus.PROCESSED.value,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def query(self, *args):
        self.query_args = args
        return self

    def filter(self, *args):
        self.filter_args = args
        return self

    def first(self):
        if self.query_args[0] is AccountStatement:
            return self.account_statement
        elif self.query_args[0] is AccountStatementLob:
            return self.account_statement_lob
        elif self.query_args[0] is BenefitProgramConfiguration:
            return self.benefit_program_configuration
        elif self.query_args[0] is Disbursement:
            return self.disbursement
        elif self.query_args[0] is DisbursementEnvelopeBatchStatus:
            return self.disbursement_envelope_batch_status
        elif self.query_args[0] is DisbursementRecon:
            return self.disbursement_recon
        elif self.query_args[0] is DisbursementBatchControl:
            return self.disbursement_batch_control
        return None

    def with_for_update(self, nowait=False):
        return self

    def populate_existing(self):
        return self

    def add(self, obj):
        self.added = True
        pass

    def add_all(self, items):
        pass

    def commit(self):
        self.committed = True

    def flush(self):
        self.flushed = True
        if hasattr(self, "disbursement_envelope_batch_status"):
            self.disbursement_envelope_batch_status.number_of_disbursements_reversed = 2


@pytest.fixture
def mock_session_maker():
    mock_session = MockSession()

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.mt940_processor.sessionmaker",
        return_value=lambda: mock_session,
    ):
        yield mock_session


@pytest.fixture
def mock_bank_connector_factory():
    mock_bank_connector = MagicMock()
    mock_bank_factory = MagicMock()
    mock_bank_factory.get_bank_connector.return_value = mock_bank_connector

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.mt940_processor.BankConnectorFactory.get_component",
        return_value=mock_bank_factory,
    ):
        yield mock_bank_connector


def test_mt940_processor_success(mock_session_maker, mock_bank_connector_factory):
    mock_bank_connector_factory.retrieve_disbursement_id.return_value = (
        "test_disbursement_id"
    )
    mock_bank_connector_factory.retrieve_beneficiary_name.return_value = (
        "Test Beneficiary"
    )

    mt940_processor_worker("test_statement_id")

    assert (
        mock_session_maker.account_statement.statement_process_status
        == ProcessStatus.PROCESSED
    )
    assert mock_session_maker.account_statement.statement_process_error_code is None
    assert isinstance(
        mock_session_maker.account_statement.statement_process_timestamp, datetime
    )
    assert mock_session_maker.committed


def test_mt940_processor_invalid_account(
    mock_session_maker, mock_bank_connector_factory
):
    mock_session_maker.benefit_program_configuration = None

    mt940_processor_worker("test_statement_id")

    assert (
        mock_session_maker.account_statement.statement_process_status
        == ProcessStatus.ERROR
    )
    assert (
        mock_session_maker.account_statement.statement_process_error_code
        == G2PBridgeErrorCodes.INVALID_ACCOUNT_NUMBER.value
    )
    assert isinstance(
        mock_session_maker.account_statement.statement_process_timestamp, datetime
    )
    assert mock_session_maker.committed


def test_mt940_processor_statement_not_found(mock_session_maker):
    mock_session_maker.account_statement = None

    mt940_processor_worker("test_statement_id")

    assert not mock_session_maker.committed


def test_mt940_processor_lob_not_found(mock_session_maker):
    mock_session_maker.account_statement_lob = None

    mt940_processor_worker("test_statement_id")

    assert not mock_session_maker.committed


def test_mt940_processor_exception(mock_session_maker, mock_bank_connector_factory):
    # Mock mt940.models.Transactions to raise an exception
    with patch("mt940.models.Transactions") as mock_transactions:
        mock_transactions.side_effect = Exception("TEST_ERROR")

        with pytest.raises(Exception, match="TEST_ERROR"):
            mt940_processor_worker("test_statement_id")

        assert (
            mock_session_maker.account_statement.statement_process_status
            == ProcessStatus.PENDING
        )
        assert (
            mock_session_maker.account_statement.statement_process_error_code
            == "TEST_ERROR"
        )
        assert isinstance(
            mock_session_maker.account_statement.statement_process_timestamp, datetime
        )
        assert mock_session_maker.committed


def test_get_disbursement_envelope_id_success(mock_session_maker):
    result = get_disbursement_envelope_id("test_disbursement_id", mock_session_maker)

    assert result == "test_envelope_id"


def test_get_disbursement_envelope_id_not_found(mock_session_maker):
    mock_session_maker.disbursement = None

    with pytest.raises(AttributeError):
        get_disbursement_envelope_id("test_disbursement_id", mock_session_maker)


def test_construct_parsed_transaction(mock_session_maker, mock_bank_connector_factory):
    mock_transaction = MagicMock()
    mock_transaction.data = {
        "amount": MagicMock(amount=100),
        "customer_reference": "test_reference",
        "bank_reference": "test_bank_ref",
        "transaction_details": "test details",
        "entry_date": datetime.now(),
        "date": datetime.now(),
    }

    mock_bank_connector_factory.retrieve_disbursement_id.return_value = (
        "test_disbursement_id"
    )
    mock_bank_connector_factory.retrieve_beneficiary_name.return_value = (
        "Test Beneficiary"
    )

    result = construct_parsed_transaction(
        mock_bank_connector_factory, "D", 1, mock_transaction, mock_session_maker
    )

    assert result["disbursement_id"] == "test_disbursement_id"
    assert result["disbursement_envelope_id"] == "test_envelope_id"
    assert result["transaction_amount"] == 100
    assert result["debit_credit_indicator"] == "D"
    assert result["beneficiary_name_from_bank"] == "Test Beneficiary"


def test_process_debit_transactions_success(
    mock_session_maker, mock_bank_connector_factory
):
    account_statement = AccountStatement(
        statement_id="test_statement_id", statement_number="123", sequence_number="1"
    )
    disbursement_error_recons = []
    disbursement_recons_d = []
    parsed_transactions_d = [
        {
            "disbursement_id": "test_disbursement_id",
            "disbursement_envelope_id": "test_envelope_id",
            "transaction_amount": 100,
            "debit_credit_indicator": "D",
            "beneficiary_name_from_bank": "Test Beneficiary",
            "remittance_reference_number": "test_bank_ref",
            "remittance_entry_sequence": 1,
            "remittance_entry_date": datetime.now(),
            "remittance_value_date": datetime.now(),
        }
    ]

    process_debit_transactions(
        account_statement,
        disbursement_error_recons,
        disbursement_recons_d,
        parsed_transactions_d,
        mock_session_maker,
        "test_statement_id",
    )

    assert len(disbursement_recons_d) == 1
    assert len(disbursement_error_recons) == 0
    assert disbursement_recons_d[0].disbursement_id == "test_disbursement_id"


def test_process_debit_transactions_invalid_disbursement(
    mock_session_maker, mock_bank_connector_factory
):
    # Set disbursement_batch_control to None for this test
    mock_session_maker.disbursement_batch_control = None

    account_statement = AccountStatement(
        statement_id="test_statement_id", statement_number="123", sequence_number="1"
    )
    disbursement_error_recons = []
    disbursement_recons_d = []
    parsed_transactions_d = [
        {
            "disbursement_id": "INVALID_ID",  # Set to an invaild id for this test
            "disbursement_envelope_id": "test_envelope_id",
            "transaction_amount": 100,
            "debit_credit_indicator": "D",
            "beneficiary_name_from_bank": "Test Beneficiary",
            "remittance_reference_number": "test_bank_ref",
            "remittance_entry_sequence": 1,
            "remittance_entry_date": datetime.now(),
            "remittance_value_date": datetime.now(),
        }
    ]

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.mt940_processor.get_bank_batch_id",
        return_value=None,
    ):
        process_debit_transactions(
            account_statement,
            disbursement_error_recons,
            disbursement_recons_d,
            parsed_transactions_d,
            mock_session_maker,
            "test_statement_id",
        )

    assert len(disbursement_recons_d) == 0
    assert len(disbursement_error_recons) == 1
    assert (
        disbursement_error_recons[0].error_reason
        == G2PBridgeErrorCodes.INVALID_DISBURSEMENT_ID
    )


def test_process_debit_transactions_duplicate(mock_session_maker):
    # Add a mock DisbursementRecon to simulate duplicate
    mock_session_maker.disbursement_recon = DisbursementRecon(
        disbursement_id="test_disbursement_id",
        remittance_statement_id="test_statement_id",
        disbursement_envelope_id="test_envelope_id",
        active=True,
        remittance_reference_number="test_ref",
        remittance_entry_sequence=1,
        remittance_entry_date=datetime.now(),
        remittance_value_date=datetime.now(),
    )

    account_statement = AccountStatement(
        statement_id="test_statement_id", statement_number="123", sequence_number="1"
    )
    disbursement_error_recons = []
    disbursement_recons_d = []
    parsed_transactions_d = [
        {
            "disbursement_id": "test_disbursement_id",
            "disbursement_envelope_id": "test_envelope_id",
            "transaction_amount": 100,
            "debit_credit_indicator": "D",
            "beneficiary_name_from_bank": "Test Beneficiary",
            "remittance_reference_number": "test_bank_ref",
            "remittance_entry_sequence": 1,
            "remittance_entry_date": datetime.now(),
            "remittance_value_date": datetime.now(),
        }
    ]

    process_debit_transactions(
        account_statement,
        disbursement_error_recons,
        disbursement_recons_d,
        parsed_transactions_d,
        mock_session_maker,
        "test_statement_id",
    )

    assert len(disbursement_recons_d) == 0
    assert len(disbursement_error_recons) == 1
    assert (
        disbursement_error_recons[0].error_reason
        == G2PBridgeErrorCodes.DUPLICATE_DISBURSEMENT
    )


def test_process_reversal_of_debits_success(mock_session_maker):
    # Add existing DisbursementRecon to mock
    mock_session_maker.disbursement_recon = DisbursementRecon(
        disbursement_id="test_disbursement_id",
        disbursement_envelope_id="test_envelope_id",
        active=True,
    )

    account_statement = AccountStatement(
        statement_id="test_statement_id", statement_number="123", sequence_number="1"
    )
    disbursement_error_recons = []
    disbursement_recons_rd = []
    parsed_transactions_rd = [
        {
            "disbursement_id": "test_disbursement_id",
            "disbursement_envelope_id": "test_envelope_id",
            "transaction_amount": 100,
            "debit_credit_indicator": "RD",
            "beneficiary_name_from_bank": "Test Beneficiary",
            "remittance_reference_number": "test_bank_ref",
            "remittance_entry_sequence": 1,
            "remittance_entry_date": datetime.now(),
            "remittance_value_date": datetime.now(),
            "reversal_entry_sequence": 1,
            "reversal_entry_date": datetime.now(),
            "reversal_value_date": datetime.now(),
            "reversal_reason": "TEST_REASON",
        }
    ]

    process_reversal_of_debits(
        account_statement,
        disbursement_error_recons,
        disbursement_recons_rd,
        parsed_transactions_rd,
        mock_session_maker,
        "test_statement_id",
    )

    assert len(disbursement_recons_rd) == 1
    assert len(disbursement_error_recons) == 0
    assert disbursement_recons_rd[0].disbursement_id == "test_disbursement_id"


def test_update_envelope_batch_status_reconciled(mock_session_maker):
    disbursement_recons = [
        DisbursementRecon(
            disbursement_envelope_id="test_envelope_id",
            disbursement_id="test_disbursement_id_1",
        ),
        DisbursementRecon(
            disbursement_envelope_id="test_envelope_id",
            disbursement_id="test_disbursement_id_2",
        ),
    ]

    update_envelope_batch_status_reconciled(disbursement_recons, mock_session_maker)

    assert (
        mock_session_maker.disbursement_envelope_batch_status.number_of_disbursements_reconciled
        == 2
    )
    assert mock_session_maker.added
    assert mock_session_maker.flushed


def test_update_envelope_batch_status_reversed(mock_session_maker):
    disbursement_recons = [
        DisbursementRecon(
            disbursement_envelope_id="test_envelope_id",
            disbursement_id="test_disbursement_id_1",
            active=True,
            remittance_reference_number="test_ref_1",
            remittance_entry_sequence=1,
            remittance_entry_date=datetime.now(),
            remittance_value_date=datetime.now(),
        ),
        DisbursementRecon(
            disbursement_envelope_id="test_envelope_id",
            disbursement_id="test_disbursement_id_2",
            active=True,
            remittance_reference_number="test_ref_2",
            remittance_entry_sequence=2,
            remittance_entry_date=datetime.now(),
            remittance_value_date=datetime.now(),
        ),
    ]

    update_envelope_batch_status_reversed(disbursement_recons, mock_session_maker)

    assert (
        mock_session_maker.disbursement_envelope_batch_status.number_of_disbursements_reversed
        == 2
    )
    assert mock_session_maker.added
