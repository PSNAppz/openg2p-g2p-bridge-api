import logging
import uuid
from datetime import datetime

from fastapi import UploadFile
from openg2p_fastapi_common.context import dbengine
from openg2p_fastapi_common.service import BaseService
from openg2p_g2p_bridge_models.errors.codes import G2PBridgeErrorCodes
from openg2p_g2p_bridge_models.models import AccountStatement, AccountStatementLob
from openg2p_g2p_bridge_models.schemas import AccountStatementResponse, ResponseStatus
from sqlalchemy.ext.asyncio import async_sessionmaker

from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class AccountStatementService(BaseService):
    async def upload_mt940(self, statement_file: UploadFile) -> str:
        try:
            statement_file = await statement_file.read()
        except Exception as e:
            _logger.error(f"Error reading file {statement_file.filename}: {str(e)}")
            raise e

        statement_id = str(uuid.uuid4())
        statement_date = datetime.utcnow()

        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            statement = AccountStatement(
                statement_id=statement_id,
                statement_date=statement_date,
                active=True,
            )
            session.add(statement)

            statement_lob = AccountStatementLob(
                statement_id=statement_id,
                statement_lob=str(statement_file),
                active=True,
            )
            session.add(statement_lob)

            await session.commit()

        return statement_id

    async def construct_account_statement_success_response(
        self, statement_id: str
    ) -> AccountStatementResponse:
        return AccountStatementResponse(
            response_status=ResponseStatus.SUCCESS,
            statement_id=statement_id,
            error_code="",
        )

    async def construct_account_statement_error_response(
        self, code: G2PBridgeErrorCodes
    ) -> AccountStatementResponse:
        return AccountStatementResponse(
            response_status=ResponseStatus.FAILURE,
            statement_id="",
            error_code=code.value,
        )
