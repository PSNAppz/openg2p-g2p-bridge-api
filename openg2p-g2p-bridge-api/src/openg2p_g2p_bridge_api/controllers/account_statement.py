from fastapi import File, UploadFile
from openg2p_fastapi_common.controller import BaseController
from openg2p_g2p_bridge_models.errors.codes import (
    G2PBridgeErrorCodes,
)
from openg2p_g2p_bridge_models.errors.exceptions import (
    AccountStatementException,
)
from openg2p_g2p_bridge_models.schemas import (
    AccountStatementResponse,
)

from openg2p_g2p_bridge_api.services import AccountStatementService


class AccountStatementController(BaseController):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.router.tags += ["G2P Bridge Account Statement"]
        self.account_statement_service = AccountStatementService.get_component()

        self.router.add_api_route(
            "/upload_mt940",
            self.upload_mt940,
            responses={200: {"model": AccountStatementResponse}},
            methods=["POST"],
        )

    async def upload_mt940(
        self,
        statement_file: UploadFile = File(...),
    ) -> AccountStatementResponse:
        try:
            account_statement_id: str = (
                await self.account_statement_service.upload_mt940(statement_file)
            )
            account_statement_response: AccountStatementResponse = await self.account_statement_service.construct_account_statement_success_response(
                account_statement_id
            )
        except AccountStatementException:
            account_statement_response: AccountStatementResponse = await self.account_statement_service.construct_account_statement_error_response(
                G2PBridgeErrorCodes.STATEMENT_UPLOAD_ERROR
            )

        return account_statement_response
