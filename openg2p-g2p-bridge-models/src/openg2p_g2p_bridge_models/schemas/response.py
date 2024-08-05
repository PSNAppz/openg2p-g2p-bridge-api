from enum import Enum
from typing import Optional

from pydantic import BaseModel

from ..errors.codes import G2PBridgeErrorCodes


class ResponseHeader(BaseModel):
    pass


class ResponseStatus(Enum):
    SUCCESS = "success"
    FAILURE = "failure"


class ResponsePagination(BaseModel):
    current_page: Optional[int] = None
    page_size: Optional[int] = None
    total_elements: Optional[int] = None


class BridgeResponse(BaseModel):
    response_header: Optional[ResponseHeader] = None
    response_status: ResponseStatus
    response_error_code: Optional[G2PBridgeErrorCodes] = None
    response_message: Optional[str] = None
    response_pagination: Optional[ResponsePagination] = ResponsePagination()
    response_payload: Optional[object] = None
