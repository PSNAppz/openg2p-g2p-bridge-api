from typing import Optional

from .response import BridgeResponse


class AccountStatementResponse(BridgeResponse):
    statement_id: Optional[str] = None
    response_error_code: Optional[str] = None
