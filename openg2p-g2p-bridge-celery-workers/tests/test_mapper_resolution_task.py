from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from openg2p_g2p_bridge_celery_workers.tasks.mapper_resolution_task import (
    make_resolve_request,
    mapper_resolution_worker,
    process_and_store_resolution,
)
from openg2p_g2p_bridge_models.models import (
    DisbursementBatchControl,
    MapperResolutionBatchStatus,
    ProcessStatus,
)


class MockSession:
    def __init__(self):
        self.committed = False
        self.flushed = False
        self.details_list = []
        self.updates = []
        self.disbursement_batch_controls = [
            DisbursementBatchControl(
                disbursement_id="test_disbursement_id",
                disbursement_envelope_id="test_envelope_id",
                beneficiary_id="test_beneficiary_id",
                bank_disbursement_batch_id=None,
                mapper_resolution_batch_id=None,
                mapper_status=ProcessStatus.PENDING,
                latest_error_code=None,
            ),
        ]
        self.mapper_resolution_batch_status = MapperResolutionBatchStatus(
            mapper_resolution_batch_id="test_batch_id",
            resolution_status=ProcessStatus.PENDING,
            resolution_time_stamp=None,
            latest_error_code=None,
            resolution_attempts=0,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    # def execute(self, *args):
    #     class ScalarResult:
    #         def __init__(self, controls):
    #             self.disbursement_batch_controls = controls

    #         def scalars(self):
    #             return self

    #         def all(self):
    #             return self.disbursement_batch_controls

    #     return ScalarResult(self.disbursement_batch_controls)
    def execute(self, *args):
        return self

    def scalars(self):
        return self

    def all(self):
        return self.disbursement_batch_controls

    def query(self, *args):
        self.query_args = args
        return self

    def filter(self, *args):
        self.filter_args = args
        return self

    def update(self, *args, **kwargs):
        self.updates.extend(arg for arg in args if isinstance(arg, dict))
        return True

    def add_all(self, items):
        self.details_list.extend(items)

    def commit(self):
        self.committed = True

    def flush(self):
        self.flushed = True


@pytest.fixture
def mock_session_maker():
    mock_session = MockSession()

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.mapper_resolution_task.sessionmaker",
        return_value=lambda: mock_session,
    ):
        yield mock_session


@pytest.fixture
def mock_resolve_helper():
    mock_helper = MagicMock()
    mock_helper_factory = MagicMock()
    mock_helper_factory.get_component.return_value = mock_helper

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.mapper_resolution_task.ResolveHelper.get_component",
        return_value=mock_helper,
    ):
        yield mock_helper


@pytest.fixture
def mock_resolve_client():
    mock_mapper_resolve_client = AsyncMock()

    with patch(
        "openg2p_g2p_bridge_celery_workers.tasks.mapper_resolution_task.MapperResolveClient",
        return_value=mock_mapper_resolve_client,
    ):
        yield mock_mapper_resolve_client


def test_mapper_resolution_worker_success(
    mock_session_maker, mock_resolve_helper, mock_resolve_client
):
    mock_response = MagicMock()
    mock_response.message.resolve_response = [
        MagicMock(
            id="test_beneficiary_id",
            fa="test_fa",
            account_provider_info=MagicMock(name="TEST_NAME"),
        )
    ]
    mock_resolve_client.resolve_request.return_value = mock_response
    mock_resolve_helper.deconstruct_fa.return_value = {
        "fa_type": "BANK",
        "account_number": "123",
        "bank_code": "ABC",
    }

    mapper_resolution_worker("test_batch_id")

    assert len(mock_session_maker.details_list) != 0
    assert (
        mock_session_maker.updates[0].get(MapperResolutionBatchStatus.resolution_status)
        == ProcessStatus.PROCESSED
    )
    assert isinstance(
        mock_session_maker.updates[0].get(
            MapperResolutionBatchStatus.resolution_time_stamp
        ),
        datetime,
    )
    assert (
        mock_session_maker.updates[0].get(MapperResolutionBatchStatus.latest_error_code)
        is None
    )

    assert (
        mock_session_maker.updates[1].get(DisbursementBatchControl.mapper_status)
        == ProcessStatus.PROCESSED.value
    )

    assert mock_session_maker.flushed
    assert mock_session_maker.committed


def test_mapper_resolution_worker_failure(
    mock_session_maker, mock_resolve_helper, mock_resolve_client
):
    # Mock failed resolve response
    mock_resolve_client.resolve_request.side_effect = Exception("TEST_ERROR")

    mapper_resolution_worker("test_batch_id")

    assert (
        mock_session_maker.updates[0].get(MapperResolutionBatchStatus.resolution_status)
        == ProcessStatus.PENDING
    )
    assert (
        mock_session_maker.updates[0].get(MapperResolutionBatchStatus.latest_error_code)
        == "Failed to resolve the request: TEST_ERROR"
    )

    assert mock_session_maker.committed


@pytest.mark.asyncio
async def test_make_resolve_request_success(mock_resolve_helper, mock_resolve_client):
    disbursement_controls = [
        DisbursementBatchControl(beneficiary_id="test_beneficiary_id")
    ]
    mock_response = "RESOLVE_RESPONSE"
    mock_resolve_client.resolve_request.return_value = mock_response

    response, error = await make_resolve_request(disbursement_controls)

    assert response == mock_response
    assert error is None


@pytest.mark.asyncio
async def test_make_resolve_request_failure(mock_resolve_helper, mock_resolve_client):
    disbursement_controls = [
        DisbursementBatchControl(beneficiary_id="test_beneficiary_id")
    ]
    mock_resolve_client.resolve_request.side_effect = Exception("TEST_ERROR")

    response, error_msg = await make_resolve_request(disbursement_controls)

    assert response is None
    assert error_msg == "Failed to resolve the request: TEST_ERROR"


def test_process_and_store_resolution_success(mock_session_maker, mock_resolve_helper):
    mock_response = MagicMock()
    mock_response.message.resolve_response = [
        MagicMock(
            id="test_beneficiary_id",
            fa="test_fa",
            account_provider_info=MagicMock(name="Test Name"),
        )
    ]
    beneficiary_map = {"test_beneficiary_id": "test_disbursement_id"}
    mock_resolve_helper.deconstruct_fa.return_value = {
        "fa_type": "BANK",
        "account_number": "123",
        "bank_code": "ABC",
        "branch_code": "XYZ",
        "mobile_number": "1234567890",
        "mobile_wallet_provider": "TEST_PROVIDER",
        "email_address": "test@example.com",
        "email_wallet_provider": "TEST_PROVIDER",
    }

    process_and_store_resolution("test_batch_id", mock_response, beneficiary_map)

    assert len(mock_session_maker.details_list) != 0
    assert (
        mock_session_maker.updates[0].get(MapperResolutionBatchStatus.resolution_status)
        == ProcessStatus.PROCESSED
    )
    assert isinstance(
        mock_session_maker.updates[0].get(
            MapperResolutionBatchStatus.resolution_time_stamp
        ),
        datetime,
    )
    assert (
        mock_session_maker.updates[0].get(MapperResolutionBatchStatus.latest_error_code)
        is None
    )

    assert (
        mock_session_maker.updates[1].get(DisbursementBatchControl.mapper_status)
        == ProcessStatus.PROCESSED.value
    )

    assert mock_session_maker.flushed
    assert mock_session_maker.committed


def test_process_and_store_resolution_failure(mock_session_maker, mock_resolve_helper):
    mock_response = MagicMock()
    mock_response.message.resolve_response = [
        MagicMock(
            id="test_beneficiary_id", fa=None  # Settup for failed resolution: L122
        )
    ]
    beneficiary_map = {"test_beneficiary_id": "test_disbursement_id"}

    process_and_store_resolution("test_batch_id", mock_response, beneficiary_map)

    assert (
        mock_session_maker.updates[0].get(MapperResolutionBatchStatus.resolution_status)
        == ProcessStatus.PENDING
    )
    assert (
        mock_session_maker.updates[0].get(MapperResolutionBatchStatus.latest_error_code)
        == "Failed to resolve the request for a beneficiary id"
    )

    assert (
        mock_session_maker.updates[1].get(DisbursementBatchControl.mapper_status)
        == ProcessStatus.ERROR.value
    )

    assert mock_session_maker.flushed
    assert mock_session_maker.committed
