import logging

from openg2p_fastapi_common.controller import BaseController
from openg2p_g2p_bridge_models.errors.exceptions import DisbursementEnvelopeException
from openg2p_g2p_bridge_models.schemas import (
    DisbursementEnvelopePayload,
    DisbursementEnvelopeRequest,
    DisbursementEnvelopeResponse,
)

from ..config import Settings
from ..services import DisbursementEnvelopeService

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class DisbursementEnvelopeController(BaseController):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.disbursement_envelope_service = DisbursementEnvelopeService.get_component()
        self.router.tags += ["G2P Bridge Disbursement Envelope"]

        self.router.add_api_route(
            "/create_disbursement_envelope",
            self.create_disbursement_envelope,
            responses={200: {"model": DisbursementEnvelopeRequest}},
            methods=["POST"],
        )
        self.router.add_api_route(
            "/cancel_disbursement_envelope",
            self.cancel_disbursement_envelope,
            responses={200: {"model": DisbursementEnvelopeRequest}},
            methods=["POST"],
        )

    async def create_disbursement_envelope(
        self, disbursement_envelope_request: DisbursementEnvelopeRequest
    ) -> DisbursementEnvelopeResponse:
        _logger.info("Creating disbursement envelope")
        try:
            disbursement_envelope_payload: DisbursementEnvelopePayload = (
                await self.disbursement_envelope_service.create_disbursement_envelope(
                    disbursement_envelope_request
                )
            )
        except DisbursementEnvelopeException as e:
            _logger.error("Error creating disbursement envelope")
            error_response: DisbursementEnvelopeResponse = await self.disbursement_envelope_service.construct_disbursement_envelope_error_response(
                e.code
            )
            return error_response

        disbursement_envelope_response: DisbursementEnvelopeResponse = await self.disbursement_envelope_service.construct_disbursement_envelope_success_response(
            disbursement_envelope_payload
        )
        _logger.info("Disbursement envelope created successfully")
        return disbursement_envelope_response

    async def cancel_disbursement_envelope(
        self, disbursement_envelope_request: DisbursementEnvelopeRequest
    ) -> DisbursementEnvelopeResponse:
        _logger.info("Cancelling disbursement envelope")
        try:
            disbursement_envelope_payload: DisbursementEnvelopePayload = (
                await self.disbursement_envelope_service.cancel_disbursement_envelope(
                    disbursement_envelope_request
                )
            )
        except DisbursementEnvelopeException as e:
            _logger.error("Error cancelling disbursement envelope")
            error_response: DisbursementEnvelopeResponse = await self.disbursement_envelope_service.construct_disbursement_envelope_error_response(
                e.code
            )
            return error_response

        disbursement_envelope_response: DisbursementEnvelopeResponse = await self.disbursement_envelope_service.construct_disbursement_envelope_success_response(
            disbursement_envelope_payload
        )
        _logger.info("Disbursement envelope cancelled successfully")
        return disbursement_envelope_response
