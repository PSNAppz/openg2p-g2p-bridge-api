import logging
from typing import List

from openg2p_fastapi_common.controller import BaseController
from openg2p_g2p_bridge_models.errors.exceptions import DisbursementException
from openg2p_g2p_bridge_models.schemas import (
    DisbursementStatusPayload,
    DisbursementStatusRequest,
    DisbursementStatusResponse,
)

from ..config import Settings
from ..services import DisbursementStatusService

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class DisbursementStatusController(BaseController):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.disbursement_service = DisbursementStatusService.get_component()
        self.router.tags += ["G2P Bridge Disbursement Status"]

        self.router.add_api_route(
            "/get_disbursement_status",
            self.get_disbursement_status,
            responses={200: {"model": DisbursementStatusResponse}},
            methods=["POST"],
        )

    async def get_disbursement_status(
        self, disbursement_status_request: DisbursementStatusRequest
    ) -> DisbursementStatusResponse:
        _logger.info("Retrieving disbursement envelope status")
        try:
            disbursement_status_payloads: List[
                DisbursementStatusPayload
            ] = await self.disbursement_service.get_disbursement_status_payloads(
                disbursement_status_request
            )
            disbursement_status_response: DisbursementStatusResponse = await self.disbursement_service.construct_disbursement_status_success_response(
                disbursement_status_request, disbursement_status_payloads
            )
            _logger.info("Disbursements cancelled successfully")
            return disbursement_status_response
        except DisbursementException as e:
            error_response: DisbursementStatusResponse = await self.disbursement_service.construct_disbursement_status_error_response(
                disbursement_status_request, e.code
            )
            return error_response
