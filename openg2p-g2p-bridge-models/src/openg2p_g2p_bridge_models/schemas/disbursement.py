import datetime
from typing import List, Optional

from pydantic import BaseModel

from ..models import CancellationStatus
from .request import BridgeRequest
from .response import BridgeResponse


class DisbursementPayload(BaseModel):
    id: Optional[str] = None
    disbursement_id: Optional[str] = None
    disbursement_envelope_id: Optional[str] = None
    beneficiary_id: Optional[str] = None
    beneficiary_name: Optional[str] = None
    disbursement_amount: Optional[float] = None
    narrative: Optional[str] = None
    receipt_time_stamp: Optional[datetime.datetime] = None
    cancellation_status: Optional[CancellationStatus] = None
    cancellation_time_stamp: Optional[datetime.datetime] = None
    response_error_codes: Optional[List[str]] = None


class DisbursementRequest(BridgeRequest):
    request_payload: List[DisbursementPayload]


class DisbursementResponse(BridgeResponse):
    response_payload: Optional[List[DisbursementPayload]] = None
