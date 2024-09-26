import logging
from typing import List

from openg2p_fastapi_common.controller import BaseController
from openg2p_g2p_bridge_models.errors.exceptions import DisbursementException
from openg2p_g2p_bridge_models.schemas import (
    DisbursementPayload,
    DisbursementRequest,
    DisbursementResponse,
)

from ..config import Settings
from ..services import DisbursementService

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class DisbursementController(BaseController):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.disbursement_service = DisbursementService.get_component()
        self.router.tags += ["G2P Bridge Disbursement Envelope"]

        self.router.add_api_route(
            "/create_disbursements",
            self.create_disbursements,
            responses={200: {"model": DisbursementResponse}},
            methods=["POST"],
        )
        self.router.add_api_route(
            "/cancel_disbursements",
            self.cancel_disbursements,
            responses={200: {"model": DisbursementResponse}},
            methods=["POST"],
        )

    async def create_disbursements(
        self, disbursement_request: DisbursementRequest
    ) -> DisbursementResponse:
        _logger.info("Creating disbursements")
        try:
            disbursement_payloads: List[
                DisbursementPayload
            ] = await self.disbursement_service.create_disbursements(
                disbursement_request
            )
        except DisbursementException as e:
            _logger.error("Error creating disbursements")
            error_response: DisbursementResponse = (
                await self.disbursement_service.construct_disbursement_error_response(
                    disbursement_request, e.code, e.disbursement_payloads
                )
            )
            return error_response

        disbursement_response: DisbursementResponse = (
            await self.disbursement_service.construct_disbursement_success_response(
                disbursement_request, disbursement_payloads
            )
        )
        _logger.info("Disbursements created successfully")

        return disbursement_response

    async def cancel_disbursements(
        self, disbursement_request: DisbursementRequest
    ) -> DisbursementResponse:
        _logger.info("Cancelling disbursements")
        try:
            disbursement_payloads: List[
                DisbursementPayload
            ] = await self.disbursement_service.cancel_disbursements(
                disbursement_request
            )
        except DisbursementException as e:
            _logger.error("Error cancelling disbursements")
            error_response: DisbursementResponse = (
                await self.disbursement_service.construct_disbursement_error_response(
                    disbursement_request, e.code, e.disbursement_payloads
                )
            )
            return error_response

        disbursement_response: DisbursementResponse = (
            await self.disbursement_service.construct_disbursement_success_response(
                disbursement_request, disbursement_payloads
            )
        )
        _logger.info("Disbursements cancelled successfully")
        return disbursement_response
