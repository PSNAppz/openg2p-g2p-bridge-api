from typing import List, Optional

import httpx
from openg2p_g2p_bridge_models.models import (
    FundsAvailableWithBankEnum,
    FundsBlockedWithBankEnum,
)
from pydantic import BaseModel

from ..bank_interface.bank_connector_interface import (
    BankConnectorInterface,
    BlockFundsResponse,
    CheckFundsResponse,
    DisbursementPaymentPayload,
    PaymentResponse,
    PaymentStatus,
)
from ..config import Settings

_config = Settings.get_config()


class BankPaymentPayload(BaseModel):
    payment_reference_number: str
    remitting_account: str
    remitting_account_currency: str
    payment_amount: float
    funds_blocked_reference_number: str
    beneficiary_name: str

    beneficiary_account: str
    beneficiary_account_currency: str
    beneficiary_account_type: str
    beneficiary_bank_code: str
    beneficiary_branch_code: str

    beneficiary_mobile_wallet_provider: Optional[str] = None
    beneficiary_phone_no: Optional[str] = None

    beneficiary_email: Optional[str] = None
    beneficiary_email_wallet_provider: Optional[str] = None

    narrative_1: Optional[str] = None
    narrative_2: Optional[str] = None
    narrative_3: Optional[str] = None
    narrative_4: Optional[str] = None
    narrative_5: Optional[str] = None
    narrative_6: Optional[str] = None

    payment_date: str


class ExampleBankConnector(BankConnectorInterface):
    def check_funds(self, account_number, currency, amount) -> CheckFundsResponse:
        try:
            with httpx.Client() as client:
                request_data = {
                    "account_number": account_number,
                    "account_currency": currency,
                    "total_funds_needed": amount,
                }
                response = client.post(
                    _config.funds_available_check_url_example_bank, json=request_data
                )
                response.raise_for_status()

                data = response.json()
                if data["status"] == "success":
                    return CheckFundsResponse(
                        status=FundsAvailableWithBankEnum.FUNDS_AVAILABLE, error_code=""
                    )
                return CheckFundsResponse(
                    status=FundsAvailableWithBankEnum.FUNDS_NOT_AVAILABLE, error_code=""
                )
        except httpx.HTTPStatusError as e:
            return CheckFundsResponse(
                status=FundsAvailableWithBankEnum.PENDING_CHECK, error_code=str(e)
            )

    def block_funds(self, account_number, currency, amount) -> BlockFundsResponse:
        try:
            with httpx.Client() as client:
                request_data = {
                    "account_number": account_number,
                    "currency": currency,
                    "amount": amount,
                }
                response = client.post(
                    _config.funds_block_url_example_bank, json=request_data
                )
                response.raise_for_status()

                data = response.json()
                if data["status"] == "success":
                    return BlockFundsResponse(
                        status=FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS,
                        block_reference_no=data["block_reference_no"],
                        error_code="",
                    )
                return BlockFundsResponse(
                    status=FundsBlockedWithBankEnum.FUNDS_BLOCK_FAILURE,
                    block_reference_no="",
                    error_code=data.get("error_code", ""),
                )
        except httpx.HTTPStatusError as e:
            return BlockFundsResponse(
                status=FundsBlockedWithBankEnum.FUNDS_BLOCK_FAILURE,
                block_reference_no="",
                error_code=str(e),
            )

    def initiate_payment(
        self, disbursement_payment_payloads: List[DisbursementPaymentPayload]
    ) -> PaymentResponse:
        try:
            with httpx.Client() as client:
                bank_payment_payloads = []
                for disbursement_payment_payload in disbursement_payment_payloads:
                    bank_payment_payload: BankPaymentPayload = BankPaymentPayload(
                        payment_reference_number=disbursement_payment_payload.disbursement_id,
                        remitting_account=disbursement_payment_payload.remitting_account,
                        remitting_account_currency=disbursement_payment_payload.remitting_account_currency,
                        payment_amount=disbursement_payment_payload.payment_amount,
                        funds_blocked_reference_number=disbursement_payment_payload.funds_blocked_reference_number,
                        beneficiary_name=disbursement_payment_payload.beneficiary_name,
                        beneficiary_account=disbursement_payment_payload.beneficiary_account,
                        beneficiary_account_currency=disbursement_payment_payload.beneficiary_account_currency,
                        beneficiary_account_type=disbursement_payment_payload.beneficiary_account_type,
                        beneficiary_bank_code=disbursement_payment_payload.beneficiary_bank_code,
                        beneficiary_branch_code=disbursement_payment_payload.beneficiary_branch_code,
                        beneficiary_mobile_wallet_provider=disbursement_payment_payload.beneficiary_mobile_wallet_provider,
                        beneficiary_phone_no=disbursement_payment_payload.beneficiary_phone_no,
                        beneficiary_email=disbursement_payment_payload.beneficiary_email,
                        beneficiary_email_wallet_provider=disbursement_payment_payload.beneficiary_email_wallet_provider,
                        payment_date=disbursement_payment_payload.payment_date,
                        narrative_1=disbursement_payment_payload.disbursement_narrative,
                        narrative_2=disbursement_payment_payload.benefit_program_mnemonic,
                        narrative_3=disbursement_payment_payload.cycle_code_mnemonic,
                        narrative_4=disbursement_payment_payload.beneficiary_id,
                        narrative_5="",
                        narrative_6="",
                        active=True,
                    )
                    bank_payment_payloads.append(bank_payment_payload.model_dump())

                request_data = {"initiate_payment_payloads": bank_payment_payloads}

                response = client.post(
                    _config.funds_disbursement_url_example_bank, json=request_data
                )
                response.raise_for_status()

                data = response.json()
                if data["status"] == "success":
                    return PaymentResponse(status=PaymentStatus.SUCCESS, error_code="")
                return PaymentResponse(
                    status=PaymentStatus.ERROR, error_code=data.get("error_message", "")
                )
        except httpx.HTTPStatusError as e:
            return PaymentResponse(status=PaymentStatus.ERROR, error_code=str(e))

    def retrieve_disbursement_id(
        self, bank_reference: str, customer_reference: str, narratives: str
    ) -> str:
        return customer_reference

    def retrieve_beneficiary_name(self, narratives: str) -> str:
        return narratives[0]

    def retrieve_reversal_reason(self, narratives: str) -> str:
        return narratives[1]
