import uuid
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from openg2p_g2p_bridge_celery_workers.tasks import mapper_resolution_worker
from openg2p_g2p_bridge_models.models import DisbursementBatchControl
from openg2p_g2pconnect_common_lib.schemas import (
    RequestHeader,
    StatusEnum,
    SyncResponseHeader,
)
from openg2p_g2pconnect_mapper_lib.client import MapperResolveClient
from openg2p_g2pconnect_mapper_lib.schemas import (
    ResolveRequest,
    ResolveRequestMessage,
    ResolveResponse,
    ResolveResponseMessage,
    SingleResolveRequest,
    SingleResolveResponse,
)


@pytest.fixture
def mock_session_maker():
    session_mock = MagicMock()
    session_mock.configure_mock(
        **{
            "execute.return_value.scalars.return_value.all.return_value": [
                DisbursementBatchControl(beneficiary_id="1", disbursement_id="101")
            ],
            "__enter__.return_value": session_mock,
            "__exit__.return_value": None,
        }
    )
    return session_mock


@pytest.fixture
def mock_resolve_helper():
    helper_mock = MagicMock()
    single_resolve_request = SingleResolveRequest(
        reference_id=str(uuid.uuid4()),
        timestamp=datetime.now(),
        id="1",
        scope="details",
    )
    helper_mock.construct_single_resolve_request.return_value = single_resolve_request
    helper_mock.construct_resolve_request.return_value = ResolveRequest(
        signature="",
        header=RequestHeader(
            message_id=str(uuid.uuid4()),
            message_ts=str(datetime.now()),
            action="resolve",
            sender_id="",
            sender_uri="",
            total_count=1,
        ),
        message=ResolveRequestMessage(
            transaction_id=str(uuid.uuid4()),
            resolve_request=[single_resolve_request],
        ),
    )
    return helper_mock


@pytest.fixture
def mock_resolve_client():
    client_mock = MagicMock(spec=MapperResolveClient)
    single_response = SingleResolveResponse(
        reference_id="ref123",
        timestamp=datetime.now(),
        fa="FA123",
        id="1",
        account_provider_info=None,
        status=StatusEnum.succ,  # Assuming you have an Enum for status
        status_reason_message="No issues.",
    )
    resolve_response_message = ResolveResponseMessage(
        transaction_id="trans123",
        correlation_id="corr123",
        resolve_response=[single_response],
    )
    resolve_response = ResolveResponse(
        header=SyncResponseHeader(
            version="1.0.0",
            message_id="",
            message_ts="",
            action="resolve",
            status=StatusEnum.succ,
        ),
        message=resolve_response_message,
    )
    client_mock.resolve_request.return_value = resolve_response
    return client_mock


@patch("openg2p_g2p_bridge_celery_tasks.app.get_engine")
@patch("openg2p_g2p_bridge_celery_tasks.helpers.ResolveHelper.get_component")
@patch("openg2p_g2pconnect_mapper_lib.client.MapperResolveClient")
@patch("sqlalchemy.orm.sessionmaker")
def test_mapper_resolve_task_success(
    mock_session_maker_func,
    mock_resolve_client_cls,
    mock_resolve_helper_func,
    mock_engine,
    mock_session_maker,
    mock_resolve_helper,
    mock_resolve_client,
):
    print("Starting test...")
    mock_session_maker_func.return_value = mock_session_maker
    mock_resolve_helper_func.return_value = mock_resolve_helper
    mock_resolve_client_cls.return_value = mock_resolve_client

    mock_resolve_client.resolve_request.return_value = MagicMock(
        message=MagicMock(
            resolve_response=[
                MagicMock(
                    id="1",
                    fa="Test FA",
                    account_provider_info=MagicMock(name="Test Provider"),
                )
            ]
        )
    )

    valid_uuid = str(uuid.uuid4())
    print("UUID for testing:", valid_uuid)

    try:
        mapper_resolution_worker(valid_uuid)
    except Exception as e:
        print("Error during task execution:", str(e))

    session_mock = mock_session_maker.__enter__.return_value
    print("Session mock add called:", session_mock.add.called)
    print("Session mock commit called:", session_mock.commit.called)
    print("API Call details:", mock_resolve_client.resolve_request.call_args_list)

    assert (
        mock_resolve_client.resolve_request.called
    ), "Resolve request was not called as expected."
    assert (
        session_mock.add.called
    ), "The session.add method was not called, which suggests the task exited early or failed."
    assert session_mock.commit.called, "The session.commit method was not called."


@patch("openg2p_g2p_bridge_celery_tasks.app.get_engine")
@patch("openg2p_g2p_bridge_celery_tasks.helpers.ResolveHelper.get_component")
@patch("openg2p_g2pconnect_mapper_lib.client.MapperResolveClient")
@patch("sqlalchemy.orm.sessionmaker")
def test_mapper_resolve_task_failure(
    mock_session_maker_func,
    mock_resolve_client_cls,
    mock_resolve_helper_func,
    mock_engine,
    mock_session_maker,
    mock_resolve_helper,
    mock_resolve_client,
):
    mock_session_maker_func.return_value = mock_session_maker
    mock_resolve_helper_func.return_value = mock_resolve_helper
    mock_resolve_client_cls.return_value = mock_resolve_client

    mock_resolve_client.resolve_request.side_effect = Exception("API failure")
    valid_uuid = str(uuid.uuid4())  # Generate a valid UUID
    mapper_resolution_worker(valid_uuid)

    session_mock = mock_session_maker.__enter__.return_value
    assert session_mock.add.called
    assert session_mock.commit.called
    assert (
        mock_resolve_client.resolve_request.called
    ), "Resolve request was not called as expected"
