from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest
from openg2p_g2p_bridge_api.controllers import DisbursementEnvelopeController
from openg2p_g2p_bridge_api.errors import (
    DisbursementEnvelopeException,
    G2PBridgeErrorCodes,
)
from openg2p_g2p_bridge_api.schemas import (
    DisbursementEnvelopePayload,
    DisbursementEnvelopeRequest,
    DisbursementEnvelopeResponse,
    ResponseStatus,
)


def mock_create_disbursement_envelope(is_valid):
    if not is_valid:
        raise DisbursementEnvelopeException(
            code=G2PBridgeErrorCodes.INVALID_PROGRAM_MNEMONIC,
            message="Invalid program mnemonic provided.",
        )
    return DisbursementEnvelopePayload(
        disbursement_envelope_id="env123",
        benefit_program_mnemonic="TEST123",
        disbursement_frequency="Monthly",
        cycle_code_mnemonic="CYCLE42",
        number_of_beneficiaries=100,
        number_of_disbursements=100,
        total_disbursement_amount=5000.00,
        disbursement_schedule_date=datetime.date(datetime.utcnow()),
    )


@pytest.mark.asyncio
@patch("openg2p_g2p_bridge_api.services.DisbursementEnvelopeService.get_component")
async def test_create_disbursement_envelope_success(mock_service_get_component):
    mock_service_instance = AsyncMock()
    mock_service_instance.create_disbursement_envelope = AsyncMock(
        return_value=mock_create_disbursement_envelope(True)
    )
    mock_service_instance.construct_disbursement_envelope_success_response = AsyncMock()

    mock_service_get_component.return_value = mock_service_instance

    expected_payload = mock_create_disbursement_envelope(True)
    expected_response = DisbursementEnvelopeResponse(
        response_status=ResponseStatus.SUCCESS, response_payload=expected_payload
    )
    mock_service_instance.construct_disbursement_envelope_success_response.return_value = (
        expected_response
    )
    controller = DisbursementEnvelopeController()
    request_payload = DisbursementEnvelopeRequest(
        request_payload=DisbursementEnvelopePayload(
            benefit_program_mnemonic="TEST123",
            disbursement_frequency="Monthly",
            cycle_code_mnemonic="CYCLE42",
            number_of_beneficiaries=100,
            number_of_disbursements=100,
            total_disbursement_amount=5000.00,
            disbursement_schedule_date=datetime.date(datetime.utcnow()),
        )
    )

    actual_response = await controller.create_disbursement_envelope(request_payload)

    assert actual_response == expected_response


@pytest.mark.asyncio
@patch("openg2p_g2p_bridge_api.services.DisbursementEnvelopeService.get_component")
async def test_create_disbursement_envelope_failure(mock_service_get_component):
    mock_service_instance = AsyncMock()

    mock_service_instance.create_disbursement_envelope.side_effect = (
        lambda request: mock_create_disbursement_envelope(False)
    )
    mock_service_instance.construct_disbursement_envelope_error_response = AsyncMock()

    mock_service_get_component.return_value = mock_service_instance

    error_response = DisbursementEnvelopeResponse(
        response_status=ResponseStatus.FAILURE,
        response_error_code=G2PBridgeErrorCodes.INVALID_PROGRAM_MNEMONIC,
    )

    mock_service_instance.construct_disbursement_envelope_error_response.return_value = (
        error_response
    )

    controller = DisbursementEnvelopeController()

    request_payload = DisbursementEnvelopeRequest(
        request_payload=DisbursementEnvelopePayload(
            benefit_program_mnemonic="",  # This should trigger the error
            disbursement_frequency="Monthly",
            cycle_code_mnemonic="CYCLE42",
            number_of_beneficiaries=100,
            number_of_disbursements=100,
            total_disbursement_amount=5000.00,
            disbursement_schedule_date=datetime.date(datetime.utcnow()),
        )
    )

    actual_response = await controller.create_disbursement_envelope(request_payload)

    assert (
        actual_response == error_response
    ), "The response did not match the expected error response."
