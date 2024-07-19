from typing import List

from openg2p_fastapi_common.controller import BaseController
from openg2p_g2p_bridge_models.errors.exceptions import DisbursementException
from openg2p_g2p_bridge_models.schemas import (
    DisbursementPayload,
    DisbursementRequest,
    DisbursementResponse,
)

from ..services import DisbursementService


class DisbursementController(BaseController):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.disbursement_service = DisbursementService.get_component()
        self.router.tags += ["G2P Bridge Disbursement Envelope"]

        self.router.add_api_route(
            "/create_disbursements",
            self.create_disbursements,
            responses={200: {"model": DisbursementRequest}},
            methods=["POST"],
        )
        self.router.add_api_route(
            "/cancel_disbursements",
            self.cancel_disbursements,
            responses={200: {"model": DisbursementRequest}},
            methods=["POST"],
        )

    async def create_disbursements(
        self, disbursement_request: DisbursementRequest
    ) -> DisbursementResponse:
        try:
            disbursement_payloads: List[
                DisbursementPayload
            ] = await self.disbursement_service.create_disbursements(
                disbursement_request
            )
        except DisbursementException as e:
            error_response: DisbursementResponse = (
                await self.disbursement_service.construct_disbursement_error_response(
                    e.code, e.disbursement_payloads
                )
            )
            return error_response

        disbursement_response: DisbursementResponse = (
            await self.disbursement_service.construct_disbursement_success_response(
                disbursement_payloads
            )
        )

        return disbursement_response

    async def cancel_disbursements(
        self, disbursement_request: DisbursementRequest
    ) -> DisbursementResponse:
        try:
            disbursement_payloads: List[
                DisbursementPayload
            ] = await self.disbursement_service.cancel_disbursements(
                disbursement_request
            )
        except DisbursementException as e:
            error_response: DisbursementResponse = (
                await self.disbursement_service.construct_disbursement_error_response(
                    e.code, e.disbursement_payloads
                )
            )
            return error_response

        disbursement_response: DisbursementResponse = (
            await self.disbursement_service.construct_disbursement_success_response(
                disbursement_payloads
            )
        )

        return disbursement_response
