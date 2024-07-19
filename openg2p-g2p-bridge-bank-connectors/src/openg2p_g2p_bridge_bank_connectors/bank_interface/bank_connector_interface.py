import enum
from datetime import datetime
from typing import List, Optional

from openg2p_fastapi_common.service import BaseService
from openg2p_g2p_bridge_models.models import (
    FundsAvailableWithBankEnum,
    FundsBlockedWithBankEnum,
)
from pydantic import BaseModel


class CheckFundsResponse(BaseModel):
    status: FundsAvailableWithBankEnum
    error_code: str


class BlockFundsResponse(BaseModel):
    status: FundsBlockedWithBankEnum
    block_reference_no: str
    error_code: str


class PaymentPayload(BaseModel):
    remitting_account: str
    remitting_account_currency: str
    payment_amount: float
    funds_blocked_reference_number: str

    beneficiary_id: str
    beneficiary_name: Optional[str] = None

    beneficiary_account: Optional[str] = None
    beneficiary_account_currency: Optional[str] = None
    beneficiary_account_type: Optional[str] = None
    beneficiary_bank_code: Optional[str] = None
    beneficiary_branch_code: Optional[str] = None

    beneficiary_mobile_wallet_provider: Optional[str] = None
    beneficiary_phone_no: Optional[str] = None

    beneficiary_email: Optional[str] = None
    beneficiary_email_wallet_provider: Optional[str] = None

    benefit_program_mnemonic: Optional[str] = None
    cycle_code_mnemonic: Optional[str] = None
    payment_date: datetime


class PaymentStatus(enum.Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"


class PaymentResponse(BaseModel):
    status: PaymentStatus
    error_code: str
    ack_reference_no: str


class BankConnectorInterface(BaseService):
    def check_funds(self, account_no, currency, amount) -> CheckFundsResponse:
        raise NotImplementedError()

    def block_funds(self, account_no, currency, amount) -> BlockFundsResponse:
        raise NotImplementedError()

    def initiate_payment(
        self, payment_payloads: List[PaymentPayload]
    ) -> PaymentResponse:
        raise NotImplementedError()
