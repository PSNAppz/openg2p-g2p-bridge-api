import datetime
from typing import Optional

from pydantic import BaseModel

from ..models import DisbursementFrequency
from .request import BridgeRequest
from .response import BridgeResponse


class DisbursementEnvelopePayload(BaseModel):
    id: Optional[str] = None
    disbursement_envelope_id: Optional[str] = None
    benefit_program_mnemonic: Optional[str] = None
    disbursement_frequency: Optional[DisbursementFrequency] = None
    cycle_code_mnemonic: Optional[str] = None
    number_of_beneficiaries: Optional[int] = None
    number_of_disbursements: Optional[int] = None
    total_disbursement_amount: Optional[float] = None
    disbursement_currency_code: Optional[str] = None
    disbursement_schedule_date: Optional[datetime.date] = None


class DisbursementEnvelopeRequest(BridgeRequest):
    request_payload: DisbursementEnvelopePayload


class DisbursementEnvelopeResponse(BridgeResponse):
    response_payload: Optional[DisbursementEnvelopePayload] = None
