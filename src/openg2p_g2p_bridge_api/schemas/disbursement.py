import datetime
from typing import List, Optional

from pydantic import BaseModel

from ..models import CancellationStatus, ReplyStatus, ShipmentStatus
from .request import BridgeRequest
from .response import BridgeResponse


class DisbursementPayload(BaseModel):
    id: Optional[str] = None
    disbursement_id: Optional[str] = None
    disbursement_envelope_id: Optional[str] = None
    beneficiary_id: Optional[int] = None
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


class DisbursementBatchStatusPayload(BaseModel):
    disbursement_id: Optional[int] = None
    disbursement_envelope_id: Optional[int] = None
    shipment_to_bank_status: Optional[ShipmentStatus] = None
    shipment_to_bank_time_stamp: Optional[datetime.datetime] = None
    reply_status_from_bank: Optional[ReplyStatus] = None
    reply_from_bank_time_stamp: Optional[datetime.datetime] = None
    reply_failure_error_code: Optional[str] = None
    reply_failure_error_message: Optional[str] = None
    reply_success_fsp_code: Optional[str] = None
    reply_success_fa: Optional[str] = None
    mapper_resolved_fa: Optional[str] = None
    mapper_resolved_phone_number: Optional[str] = None
    mapper_resolved_name: Optional[str] = None
    mapper_resolved_timestamp: Optional[datetime.datetime] = None
    mapper_resolved_retries: Optional[int] = None


class DisbursementBatchStatusRequest(BaseModel):
    request_payload: DisbursementBatchStatusPayload


class DisbursementBatchStatusResponse(BaseModel):
    response_payload: Optional[DisbursementBatchStatusPayload] = None
