from typing import Optional

from pydantic import BaseModel


class RequestHeader(BaseModel):
    pass


class RequestPagination(BaseModel):
    request_page: Optional[int]
    page_size: Optional[int]


class BridgeRequest(BaseModel):
    request_header: RequestHeader
    request_pagination: RequestPagination
    request_payload: object
