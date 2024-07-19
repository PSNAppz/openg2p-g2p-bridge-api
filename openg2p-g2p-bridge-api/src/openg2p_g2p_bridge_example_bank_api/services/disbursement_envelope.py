import time
from datetime import datetime

from openg2p_fastapi_common.context import dbengine
from openg2p_fastapi_common.service import BaseService
from openg2p_g2p_bridge_models.errors.codes import G2PBridgeErrorCodes
from openg2p_g2p_bridge_models.errors.exceptions import DisbursementEnvelopeException
from openg2p_g2p_bridge_models.models import (
    BenefitProgramConfiguration,
    CancellationStatus,
    DisbursementEnvelope,
    DisbursementEnvelopeBatchStatus,
    DisbursementFrequency,
    FundsAvailableWithBankEnum,
    FundsBlockedWithBankEnum,
)
from openg2p_g2p_bridge_models.schemas import (
    DisbursementEnvelopePayload,
    DisbursementEnvelopeRequest,
    DisbursementEnvelopeResponse,
    ResponseStatus,
)
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.future import select


class DisbursementEnvelopeService(BaseService):
    async def create_disbursement_envelope(
        self, disbursement_envelope_request: DisbursementEnvelopeRequest
    ) -> DisbursementEnvelopePayload:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            try:
                await self.validate_envelope_request(disbursement_envelope_request)
            except DisbursementEnvelopeException as e:
                raise e

            disbursement_envelope: DisbursementEnvelope = await self.construct_disbursement_envelope(
                disbursement_envelope_payload=disbursement_envelope_request.request_payload
            )

            disbursement_envelope_batch_status: DisbursementEnvelopeBatchStatus = (
                await self.construct_disbursement_envelope_batch_status(
                    disbursement_envelope, session
                )
            )

            session.add(disbursement_envelope)
            session.add(disbursement_envelope_batch_status)

            await session.commit()

            disbursement_envelope_payload: DisbursementEnvelopePayload = (
                disbursement_envelope_request.request_payload
            )
            disbursement_envelope_payload.disbursement_envelope_id = (
                disbursement_envelope.disbursement_envelope_id
            )

            return disbursement_envelope_payload

    async def cancel_disbursement_envelope(
        self, disbursement_envelope_request: DisbursementEnvelopeRequest
    ) -> DisbursementEnvelopePayload:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            disbursement_envelope_payload: DisbursementEnvelopePayload = (
                disbursement_envelope_request.request_payload
            )
            disbursement_envelope_id: str = (
                disbursement_envelope_payload.disbursement_envelope_id
            )

            disbursement_envelope: DisbursementEnvelope = (
                await session.execute(
                    select(DisbursementEnvelope).where(
                        DisbursementEnvelope.disbursement_envelope_id
                        == disbursement_envelope_id
                    )
                )
            ).scalar()

            if disbursement_envelope is None:
                raise DisbursementEnvelopeException(
                    G2PBridgeErrorCodes.DISBURSEMENT_ENVELOPE_NOT_FOUND
                )

            if (
                disbursement_envelope.cancellation_status
                == CancellationStatus.Cancelled.value
            ):
                raise DisbursementEnvelopeException(
                    G2PBridgeErrorCodes.DISBURSEMENT_ENVELOPE_ALREADY_CANCELED
                )

            disbursement_envelope.cancellation_status = (
                CancellationStatus.Cancelled.value
            )
            disbursement_envelope.cancellation_timestamp = datetime.utcnow()

            await session.commit()

            return disbursement_envelope_payload

    async def construct_disbursement_envelope_success_response(
        self, disbursement_envelope_payload: DisbursementEnvelopePayload
    ) -> DisbursementEnvelopeResponse:
        disbursement_envelope_response: DisbursementEnvelopeResponse = (
            DisbursementEnvelopeResponse(
                response_status=ResponseStatus.SUCCESS,
                response_payload=disbursement_envelope_payload,
            )
        )
        return disbursement_envelope_response

    async def construct_disbursement_envelope_error_response(
        self, error_code: G2PBridgeErrorCodes
    ) -> DisbursementEnvelopeResponse:
        disbursement_envelope_response: DisbursementEnvelopeResponse = (
            DisbursementEnvelopeResponse(
                response_status=ResponseStatus.FAILURE,
                response_error_code=error_code.value,
            )
        )

        return disbursement_envelope_response

    # noinspection PyMethodMayBeStatic
    async def validate_envelope_request(
        self, disbursement_envelope_request: DisbursementEnvelopeRequest
    ) -> bool:
        disbursement_envelope_payload: DisbursementEnvelopePayload = (
            disbursement_envelope_request.request_payload
        )
        if (
            disbursement_envelope_payload.benefit_program_mnemonic is None
            or disbursement_envelope_payload.benefit_program_mnemonic == ""
        ):
            raise DisbursementEnvelopeException(
                G2PBridgeErrorCodes.INVALID_PROGRAM_MNEMONIC
            )
        if (
            disbursement_envelope_payload.disbursement_frequency
            not in DisbursementFrequency
        ):
            raise DisbursementEnvelopeException(
                G2PBridgeErrorCodes.INVALID_DISBURSEMENT_FREQUENCY
            )
        if (
            disbursement_envelope_payload.cycle_code_mnemonic is None
            or disbursement_envelope_payload.cycle_code_mnemonic == ""
        ):
            raise DisbursementEnvelopeException(
                G2PBridgeErrorCodes.INVALID_CYCLE_CODE_MNEMONIC
            )
        if (
            disbursement_envelope_payload.number_of_beneficiaries is None
            or disbursement_envelope_payload.number_of_beneficiaries < 1
        ):
            raise DisbursementEnvelopeException(
                G2PBridgeErrorCodes.INVALID_NO_OF_BENEFICIARIES
            )
        if (
            disbursement_envelope_payload.number_of_disbursements is None
            or disbursement_envelope_payload.number_of_disbursements < 1
        ):
            raise DisbursementEnvelopeException(
                G2PBridgeErrorCodes.INVALID_NO_OF_DISBURSEMENTS
            )
        if (
            disbursement_envelope_payload.total_disbursement_amount is None
            or disbursement_envelope_payload.total_disbursement_amount < 0
        ):
            raise DisbursementEnvelopeException(
                G2PBridgeErrorCodes.INVALID_TOTAL_DISBURSEMENT_AMOUNT
            )
        if (
            disbursement_envelope_payload.disbursement_schedule_date is None
            or disbursement_envelope_payload.disbursement_schedule_date
            < datetime.date(datetime.utcnow())  # TODO: Add a delta of x days
        ):
            raise DisbursementEnvelopeException(
                G2PBridgeErrorCodes.INVALID_DISBURSEMENT_SCHEDULE_DATE
            )

        return True

    # noinspection PyMethodMayBeStatic
    async def construct_disbursement_envelope(
        self, disbursement_envelope_payload: DisbursementEnvelopePayload
    ) -> DisbursementEnvelope:
        disbursement_envelope: DisbursementEnvelope = DisbursementEnvelope(
            disbursement_envelope_id=str(int(time.time() * 1000)),
            benefit_program_mnemonic=disbursement_envelope_payload.benefit_program_mnemonic,
            disbursement_frequency=disbursement_envelope_payload.disbursement_frequency.value,
            cycle_code_mnemonic=disbursement_envelope_payload.cycle_code_mnemonic,
            number_of_beneficiaries=disbursement_envelope_payload.number_of_beneficiaries,
            number_of_disbursements=disbursement_envelope_payload.number_of_disbursements,
            total_disbursement_amount=disbursement_envelope_payload.total_disbursement_amount,
            disbursement_schedule_date=disbursement_envelope_payload.disbursement_schedule_date,
            receipt_time_stamp=datetime.utcnow(),
            cancellation_status=CancellationStatus.Not_Cancelled.value,
            active=True,
        )
        disbursement_envelope_payload.id = disbursement_envelope.id
        disbursement_envelope_payload.disbursement_envelope_id = (
            disbursement_envelope.disbursement_envelope_id
        )
        return disbursement_envelope

    # noinspection PyMethodMayBeStatic
    async def construct_disbursement_envelope_batch_status(
        self, disbursement_envelope: DisbursementEnvelope, session
    ) -> DisbursementEnvelopeBatchStatus:
        benefit_program_configuration: BenefitProgramConfiguration = (
            (
                await session.execute(
                    select(BenefitProgramConfiguration).where(
                        BenefitProgramConfiguration.benefit_program_mnemonic
                        == disbursement_envelope.benefit_program_mnemonic
                    )
                )
            )
            .scalars()
            .first()
        )
        disbursement_envelope_batch_status: DisbursementEnvelopeBatchStatus = DisbursementEnvelopeBatchStatus(
            disbursement_envelope_id=disbursement_envelope.disbursement_envelope_id,
            number_of_disbursements_received=0,
            total_disbursement_amount_received=0,
            funds_available_with_bank=FundsAvailableWithBankEnum.PENDING_CHECK.value,
            funds_available_latest_timestamp=datetime.utcnow(),
            funds_available_latest_error_code="",
            funds_available_retries=0,
            funds_blocked_with_bank=FundsBlockedWithBankEnum.PENDING_CHECK.value,
            funds_blocked_latest_timestamp=datetime.utcnow(),
            funds_blocked_retries=0,
            funds_blocked_latest_error_code="",
            active=True,
            id_mapper_resolution_required=benefit_program_configuration.id_mapper_resolution_required,
        )
        return disbursement_envelope_batch_status
