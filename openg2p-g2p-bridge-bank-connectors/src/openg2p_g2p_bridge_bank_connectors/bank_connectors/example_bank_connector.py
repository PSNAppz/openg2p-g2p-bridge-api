from typing import List

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


class ExampleBankConnector(BankConnectorInterface):
    def check_funds(self, account_no, currency, amount) -> CheckFundsResponse:
        print("EXAMPLE BANK CONNECTOR: Checking funds")

        return CheckFundsResponse(
            status=FundsAvailableWithBankEnum.FUNDS_AVAILABLE, error_code=""
        )

    def block_funds(self, account_no, currency, amount) -> BlockFundsResponse:
        print("EXAMPLE BANK CONNECTOR: Blocking funds")
        return BlockFundsResponse(
            status=FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS,
            block_reference_no="REF123",
            error_code="",
        )

    def initiate_payment(
        self, payment_payloads: List[PaymentPayload]
    ) -> PaymentResponse:
        print("EXAMPLE BANK CONNECTOR: Initiating payment")
        print("PAYMENT PAYLOADS:", payment_payloads)
        return PaymentResponse(
            status=PaymentStatus.SUCCESS, error_code="", ack_reference_no="ACK123"
        )
