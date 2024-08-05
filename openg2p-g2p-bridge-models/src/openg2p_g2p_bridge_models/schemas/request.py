from typing import Optional

from pydantic import BaseModel


class RequestHeader(BaseModel):
    pass


class RequestPagination(BaseModel):
    request_page: Optional[int]
    page_size: Optional[int]


class BridgeRequest(BaseModel):
    request_header: Optional[RequestHeader] = None
    request_pagination: Optional[RequestPagination] = None
    request_payload: object
