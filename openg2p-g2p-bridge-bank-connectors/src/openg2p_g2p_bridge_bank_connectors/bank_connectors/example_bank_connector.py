from typing import List

import httpx
from openg2p_g2p_bridge_models.models import (
    FundsAvailableWithBankEnum,
    FundsBlockedWithBankEnum,
)

from ..bank_interface.bank_connector_interface import (
    BankConnectorInterface,
    BlockFundsResponse,
    CheckFundsResponse,
    PaymentPayload,
    PaymentResponse,
    PaymentStatus,
)
from ..config import Settings

_config = Settings.get_config()


class ExampleBankConnector(BankConnectorInterface):
    def check_funds(self, account_no, currency, amount) -> CheckFundsResponse:
        try:
            with httpx.Client() as client:
                request_data = {
                    "account_number": account_no,
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

    def block_funds(self, account_no, currency, amount) -> BlockFundsResponse:
        try:
            with httpx.Client() as client:
                request_data = {
                    "account_number": account_no,
                    "account_currency": currency,
                    "total_funds_needed": amount,
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
        self, payment_payloads: List[PaymentPayload]
    ) -> PaymentResponse:
        try:
            with httpx.Client() as client:
                request_data = {
                    "payment_payloads": [
                        payload.model_dump() for payload in payment_payloads
                    ]
                }
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
