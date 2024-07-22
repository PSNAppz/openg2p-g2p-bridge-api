from datetime import datetime

from pydantic import BaseModel


class CheckFundRequest(BaseModel):
    account_number: str
    account_currency: str
    total_funds_needed: float


class CheckFundResponse(BaseModel):
    account_number: str
    has_sufficient_funds: bool


class BlockFundsRequest(BaseModel):
    account_no: str
    currency: str
    amount: float


class BlockFundsResponse(BaseModel):
    block_reference_no: str


class InitiatePaymentPayload(BaseModel):
    remitting_account: str
    remitting_account_currency: str
    payment_amount: float
    funds_blocked_reference_number: str
    beneficiary_id: str
    beneficiary_name: str
    beneficiary_account: str
    beneficiary_account_currency: str
    beneficiary_account_type: str
    beneficiary_bank_code: str
    beneficiary_branch_code: str
    benefit_program_mnemonic: str
    cycle_code_mnemonic: str
    payment_date: datetime


class InitiatorPaymentResponse(BaseModel):
    status: str
    error_message: str
