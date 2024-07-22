from openg2p_fastapi_common.context import dbengine
from openg2p_fastapi_common.controller import BaseController
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.future import select

from ..models import BenefitProgram, FundBlock, InitiatePaymentRequest
from ..schemas import InitiatePaymentPayload, InitiatorPaymentResponse


class PaymentController(BaseController):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.router.tags += ["Payments Management"]

        self.router.add_api_route(
            "/initiate_payment",
            self.initiate_payment,
            response_model=InitiatorPaymentResponse,
            methods=["POST"],
        )

    async def initiate_payment(
        self, initiate_payment_payload: InitiatePaymentPayload
    ) -> InitiatorPaymentResponse:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            fund_block_stmt = select(FundBlock).where(
                FundBlock.block_reference_no
                == initiate_payment_payload.funds_blocked_reference_number
            )
            fund_block_result = await session.execute(fund_block_stmt)
            fund_block = fund_block_result.scalars().first()

            if (
                not fund_block
                or fund_block.amount < initiate_payment_payload.payment_amount
                or fund_block.currency
                != initiate_payment_payload.remitting_account_currency
            ):
                return InitiatorPaymentResponse(
                    status="failed",
                    error_message="Invalid funds block reference or mismatch in details",
                )

            if initiate_payment_payload.benefit_program_mnemonic:
                program_stmt = select(BenefitProgram).where(
                    (
                        BenefitProgram.program_mnemonic
                        == initiate_payment_payload.benefit_program_mnemonic
                    )
                    & (
                        BenefitProgram.funding_account_number
                        == initiate_payment_payload.remitting_account
                    )
                    & (
                        BenefitProgram.funding_account_currency
                        == initiate_payment_payload.remitting_account_currency
                    )
                )
                program_result = await session.execute(program_stmt)
                benefit_program = program_result.scalars().first()
                if not benefit_program:
                    return InitiatorPaymentResponse(
                        status="failed",
                        error_message="Invalid benefit program mnemonic",
                    )

            payment = InitiatePaymentRequest(
                remitting_account=initiate_payment_payload.remitting_account,
                remitting_account_currency=initiate_payment_payload.remitting_account_currency,
                payment_amount=initiate_payment_payload.payment_amount,
                funds_blocked_reference_number=initiate_payment_payload.funds_blocked_reference_number,
                beneficiary_id=initiate_payment_payload.beneficiary_id,
                beneficiary_name=initiate_payment_payload.beneficiary_name,
                beneficiary_account=initiate_payment_payload.beneficiary_account,
                beneficiary_account_currency=initiate_payment_payload.beneficiary_account_currency,
                beneficiary_account_type=initiate_payment_payload.beneficiary_account_type,
                beneficiary_bank_code=initiate_payment_payload.beneficiary_bank_code,
                beneficiary_branch_code=initiate_payment_payload.beneficiary_branch_code,
                benefit_program_mnemonic=initiate_payment_payload.benefit_program_mnemonic,
                cycle_code_mnemonic=initiate_payment_payload.cycle_code_mnemonic,
                payment_date=initiate_payment_payload.payment_date,
                active=True,
            )
            session.add(payment)

            await session.commit()

            return InitiatorPaymentResponse(status="success", error_message="")
