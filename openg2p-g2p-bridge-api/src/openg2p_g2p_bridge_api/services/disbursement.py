import logging
import time
import uuid
from datetime import datetime
from typing import List

from openg2p_fastapi_common.context import dbengine
from openg2p_fastapi_common.service import BaseService
from openg2p_g2p_bridge_models.errors.codes import G2PBridgeErrorCodes
from openg2p_g2p_bridge_models.errors.exceptions import DisbursementException
from openg2p_g2p_bridge_models.models import (
    BankDisbursementBatchStatus,
    CancellationStatus,
    Disbursement,
    DisbursementBatchControl,
    DisbursementCancellationStatus,
    DisbursementEnvelope,
    DisbursementEnvelopeBatchStatus,
    ProcessStatus,
    MapperResolutionBatchStatus,
    ProcessStatus,
)
from openg2p_g2p_bridge_models.schemas import (
    DisbursementPayload,
    DisbursementRequest,
    DisbursementResponse,
    ResponseStatus,
)
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.future import select

from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class DisbursementService(BaseService):
    async def create_disbursements(
        self, disbursement_request: DisbursementRequest
    ) -> List[DisbursementPayload]:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            try:
                await self.validate_disbursement_envelope(
                    session=session,
                    disbursement_payloads=disbursement_request.request_payload,
                )
            except DisbursementException as e:
                raise e
            is_error_free = await self.validate_disbursement_request(
                disbursement_payloads=disbursement_request.request_payload
            )

            if not is_error_free:
                raise DisbursementException(
                    code=G2PBridgeErrorCodes.INVALID_DISBURSEMENT_PAYLOAD,
                    disbursement_payloads=disbursement_request.request_payload,
                )
            disbursements: List[Disbursement] = await self.construct_disbursements(
                disbursement_payloads=disbursement_request.request_payload
            )
            disbursement_batch_controls: List[
                DisbursementBatchControl
            ] = await self.construct_disbursement_batch_controls(
                disbursements=disbursements
            )

            disbursement_envelope_batch_status = (
                await self.update_disbursement_envelope_batch_status(
                    disbursements, session
                )
            )
            session.add_all(disbursements)
            session.add_all(disbursement_batch_controls)
            session.add(disbursement_envelope_batch_status)

            if disbursement_envelope_batch_status.id_mapper_resolution_required:
                mapper_resolution_batch_status: MapperResolutionBatchStatus = (
                    MapperResolutionBatchStatus(
                        mapper_resolution_batch_id=disbursement_batch_controls[
                            0
                        ].mapper_resolution_batch_id,
                        resolution_status=ProcessStatus.PENDING,
                        latest_error_code="",
                        active=True,
                    )
                )
                session.add(mapper_resolution_batch_status)
                _logger.info("ID Mapper Resolution Batch Status Created!")

            bank_disbursement_batch_status: BankDisbursementBatchStatus = (
                BankDisbursementBatchStatus(
                    bank_disbursement_batch_id=disbursement_batch_controls[
                        0
                    ].bank_disbursement_batch_id,
                    disbursement_envelope_id=disbursement_batch_controls[
                        0
                    ].disbursement_envelope_id,
                    disbursement_status=ProcessStatus.PENDING,
                    latest_error_code="",
                    disbursement_attempts=0,
                    active=True,
                )
            )

            session.add(bank_disbursement_batch_status)
            await session.commit()

            return disbursement_request.request_payload

    async def update_disbursement_envelope_batch_status(self, disbursements, session):
        disbursement_envelope_batch_status = (
            (
                await session.execute(
                    select(DisbursementEnvelopeBatchStatus).where(
                        DisbursementEnvelopeBatchStatus.disbursement_envelope_id
                        == str(disbursements[0].disbursement_envelope_id)
                    )
                )
            )
            .scalars()
            .first()
        )
        disbursement_envelope_batch_status.number_of_disbursements_received += len(
            disbursements
        )
        disbursement_envelope_batch_status.total_disbursement_amount_received += sum(
            [disbursement.disbursement_amount for disbursement in disbursements]
        )
        return disbursement_envelope_batch_status

    async def construct_disbursements(
        self, disbursement_payloads: List[DisbursementPayload]
    ) -> List[Disbursement]:
        disbursements: List[Disbursement] = []
        for disbursement_payload in disbursement_payloads:
            disbursement = Disbursement(
                disbursement_id=str(int(time.time() * 1000)),
                disbursement_envelope_id=str(
                    disbursement_payload.disbursement_envelope_id
                ),
                beneficiary_id=disbursement_payload.beneficiary_id,
                beneficiary_name=disbursement_payload.beneficiary_name,
                disbursement_amount=disbursement_payload.disbursement_amount,
                narrative=disbursement_payload.narrative,
                active=True,
            )
            disbursement_payload.id = disbursement.id
            disbursement_payload.disbursement_id = disbursement.disbursement_id
            disbursements.append(disbursement)
        return disbursements

    async def construct_disbursement_batch_controls(
        self, disbursements: List[Disbursement]
    ):
        disbursement_batch_controls = []
        mapper_resolution_batch_id = str(uuid.uuid4())
        bank_disbursement_batch_id = str(uuid.uuid4())
        for disbursement in disbursements:
            disbursement_batch_control = DisbursementBatchControl(
                disbursement_id=disbursement.disbursement_id,
                disbursement_envelope_id=str(disbursement.disbursement_envelope_id),
                beneficiary_id=disbursement.beneficiary_id,
                bank_disbursement_batch_id=bank_disbursement_batch_id,
                mapper_resolution_batch_id=mapper_resolution_batch_id,
                active=True,
            )
            disbursement_batch_controls.append(disbursement_batch_control)
        return disbursement_batch_controls

    async def validate_disbursement_request(
        self, disbursement_payloads: List[DisbursementPayload]
    ):
        absolutely_no_error = True

        for disbursement_payload in disbursement_payloads:
            disbursement_payload.response_error_codes = []
            if disbursement_payload.disbursement_envelope_id is None:
                disbursement_payload.response_error_codes.append(
                    G2PBridgeErrorCodes.INVALID_DISBURSEMENT_ENVELOPE_ID
                )
            if disbursement_payload.disbursement_amount <= 0:
                disbursement_payload.response_error_codes.append(
                    G2PBridgeErrorCodes.INVALID_DISBURSEMENT_AMOUNT
                )
            if disbursement_payload.beneficiary_id is None:
                disbursement_payload.response_error_codes.append(
                    G2PBridgeErrorCodes.INVALID_BENEFICIARY_ID
                )
            if (
                disbursement_payload.beneficiary_name is None
                or disbursement_payload.beneficiary_name == ""
            ):
                disbursement_payload.response_error_codes.append(
                    G2PBridgeErrorCodes.INVALID_BENEFICIARY_NAME
                )
            if (
                disbursement_payload.narrative is None
                or disbursement_payload.narrative == ""
            ):
                disbursement_payload.response_error_codes.append(
                    G2PBridgeErrorCodes.INVALID_NARRATIVE
                )

            if len(disbursement_payload.response_error_codes) > 0:
                absolutely_no_error = False

        return absolutely_no_error

    async def validate_disbursement_envelope(
        self, session, disbursement_payloads: List[DisbursementPayload]
    ):
        disbursement_envelope_id = disbursement_payloads[0].disbursement_envelope_id
        if not all(
            disbursement_payload.disbursement_envelope_id == disbursement_envelope_id
            for disbursement_payload in disbursement_payloads
        ):
            raise DisbursementException(
                G2PBridgeErrorCodes.MULTIPLE_ENVELOPES_FOUND,
                disbursement_payloads,
            )
        disbursement_envelope = (
            (
                await session.execute(
                    select(DisbursementEnvelope).where(
                        DisbursementEnvelope.disbursement_envelope_id
                        == str(disbursement_envelope_id)
                    )
                )
            )
            .scalars()
            .first()
        )
        if not disbursement_envelope:
            raise DisbursementException(
                G2PBridgeErrorCodes.DISBURSEMENT_ENVELOPE_NOT_FOUND,
                disbursement_payloads,
            )

        if disbursement_envelope.cancellation_status == CancellationStatus.Cancelled:
            raise DisbursementException(
                G2PBridgeErrorCodes.DISBURSEMENT_ENVELOPE_ALREADY_CANCELED,
                disbursement_payloads,
            )

        disbursement_envelope_batch_status = (
            (
                await session.execute(
                    select(DisbursementEnvelopeBatchStatus).where(
                        DisbursementEnvelopeBatchStatus.disbursement_envelope_id
                        == str(disbursement_envelope_id)
                    )
                )
            )
            .scalars()
            .first()
        )

        no_of_disbursements_after_this_request = (
            len(disbursement_payloads)
            + disbursement_envelope_batch_status.number_of_disbursements_received
        )
        total_disbursement_amount_after_this_request = (
            sum(
                [
                    disbursement_payload.disbursement_amount
                    for disbursement_payload in disbursement_payloads
                ]
            )
            + disbursement_envelope_batch_status.total_disbursement_amount_received
        )

        if (
            no_of_disbursements_after_this_request
            > disbursement_envelope.number_of_disbursements
        ):
            raise DisbursementException(
                G2PBridgeErrorCodes.NO_OF_DISBURSEMENTS_EXCEEDS_DECLARED,
                disbursement_payloads,
            )

        if (
            total_disbursement_amount_after_this_request
            > disbursement_envelope.total_disbursement_amount
        ):
            raise DisbursementException(
                G2PBridgeErrorCodes.TOTAL_DISBURSEMENT_AMOUNT_EXCEEDS_DECLARED,
                disbursement_payloads,
            )

        return True

    async def construct_disbursement_error_response(
        self,
        code: G2PBridgeErrorCodes,
        disbursement_payloads: List[DisbursementPayload],
    ) -> DisbursementResponse:
        disbursement_response: DisbursementResponse = DisbursementResponse(
            response_status=ResponseStatus.FAILURE,
            response_payload=disbursement_payloads,
            response_error_code=code.value,
        )

        return disbursement_response

    async def construct_disbursement_success_response(
        self, disbursement_payloads: List[DisbursementPayload]
    ) -> DisbursementResponse:
        disbursement_response: DisbursementResponse = DisbursementResponse(
            response_status=ResponseStatus.SUCCESS,
            response_payload=disbursement_payloads,
        )

        return disbursement_response

    async def cancel_disbursements(
        self, disbursement_request: DisbursementRequest
    ) -> List[DisbursementPayload]:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            is_payload_valid = await self.validate_request_payload(
                disbursement_payloads=disbursement_request.request_payload
            )

            if not is_payload_valid:
                raise DisbursementException(
                    code=G2PBridgeErrorCodes.INVALID_DISBURSEMENT_PAYLOAD,
                    disbursement_payloads=disbursement_request.request_payload,
                )

            disbursements_in_db: List[
                Disbursement
            ] = await self.fetch_disbursements_from_db(disbursement_request, session)
            if not disbursements_in_db:
                raise DisbursementException(
                    code=G2PBridgeErrorCodes.INVALID_DISBURSEMENT_ID,
                    disbursement_payloads=disbursement_request.request_payload,
                )

            try:
                await self.check_for_single_envelope(
                    disbursements_in_db, disbursement_request.request_payload
                )
            except DisbursementException as e:
                raise e

            try:
                await self.validate_envelope_for_disbursement_cancellation(
                    disbursements_in_db=disbursements_in_db,
                    disbursement_payloads=disbursement_request.request_payload,
                    session=session,
                )
            except DisbursementException as e:
                raise e

            invalid_disbursements_exist = await self.check_for_invalid_disbursements(
                disbursement_request,
                disbursements_in_db,
            )

            if invalid_disbursements_exist:
                raise DisbursementException(
                    code=G2PBridgeErrorCodes.INVALID_DISBURSEMENT_PAYLOAD,
                    disbursement_payloads=disbursement_request.request_payload,
                )

            for disbursement in disbursements_in_db:
                disbursement.cancellation_status = (
                    DisbursementCancellationStatus.CANCELLED
                )
                disbursement.cancellation_time_stamp = datetime.now()

            disbursement_envelope_batch_status = (
                (
                    await session.execute(
                        select(DisbursementEnvelopeBatchStatus).where(
                            DisbursementEnvelopeBatchStatus.disbursement_envelope_id
                            == str(disbursements_in_db[0].disbursement_envelope_id)
                        )
                    )
                )
                .scalars()
                .first()
            )

            disbursement_envelope_batch_status.number_of_disbursements_received -= len(
                disbursements_in_db
            )
            disbursement_envelope_batch_status.total_disbursement_amount_received -= (
                sum(
                    [
                        disbursement.disbursement_amount
                        for disbursement in disbursements_in_db
                    ]
                )
            )

            session.add_all(disbursements_in_db)
            session.add(disbursement_envelope_batch_status)
            await session.commit()

            return disbursement_request.request_payload

    async def check_for_single_envelope(
        self, disbursements_in_db, disbursement_payloads
    ):
        disbursement_envelope_ids = {
            disbursement.disbursement_envelope_id
            for disbursement in disbursements_in_db
        }
        if len(disbursement_envelope_ids) > 1:
            raise DisbursementException(
                G2PBridgeErrorCodes.MULTIPLE_ENVELOPES_FOUND,
                disbursement_payloads,
            )
        return True

    async def check_for_invalid_disbursements(
        self, disbursement_request, disbursements_in_db
    ) -> bool:
        invalid_disbursements_exist = False
        for disbursement_payload in disbursement_request.request_payload:
            if disbursement_payload.disbursement_id not in [
                disbursement.disbursement_id for disbursement in disbursements_in_db
            ]:
                invalid_disbursements_exist = True
                disbursement_payload.response_error_codes.append(
                    G2PBridgeErrorCodes.INVALID_DISBURSEMENT_ID.value
                )
            if disbursement_payload.disbursement_id in [
                disbursement.disbursement_id
                for disbursement in disbursements_in_db
                if disbursement.cancellation_status
                == DisbursementCancellationStatus.CANCELLED
            ]:
                invalid_disbursements_exist = True
                disbursement_payload.response_error_codes.append(
                    G2PBridgeErrorCodes.DISBURSEMENT_ALREADY_CANCELED.value
                )
        return invalid_disbursements_exist

    async def fetch_disbursements_from_db(
        self, disbursement_request, session
    ) -> List[Disbursement]:
        disbursements_in_db = (
            (
                await session.execute(
                    select(Disbursement).where(
                        Disbursement.disbursement_id.in_(
                            [
                                str(disbursement_payload.disbursement_id)
                                for disbursement_payload in disbursement_request.request_payload
                            ]
                        )
                    )
                )
            )
            .scalars()
            .all()
        )
        return disbursements_in_db

    async def validate_envelope_for_disbursement_cancellation(
        self,
        disbursements_in_db,
        disbursement_payloads: List[DisbursementPayload],
        session,
    ):
        disbursement_envelope = (
            (
                await session.execute(
                    select(DisbursementEnvelope).where(
                        DisbursementEnvelope.disbursement_envelope_id
                        == str(disbursements_in_db[0].disbursement_envelope_id)
                    )
                )
            )
            .scalars()
            .first()
        )
        if not disbursement_envelope:
            raise DisbursementException(
                G2PBridgeErrorCodes.DISBURSEMENT_ENVELOPE_NOT_FOUND,
                disbursement_payloads,
            )

        if disbursement_envelope.cancellation_status == CancellationStatus.Cancelled:
            raise DisbursementException(
                G2PBridgeErrorCodes.DISBURSEMENT_ENVELOPE_ALREADY_CANCELED,
                disbursement_payloads,
            )

        if disbursement_envelope.disbursement_schedule_date <= datetime.now().date():
            raise DisbursementException(
                G2PBridgeErrorCodes.DISBURSEMENT_ENVELOPE_SCHEDULE_DATE_REACHED,
                disbursement_payloads,
            )

        disbursement_envelope_batch_status = (
            (
                await session.execute(
                    select(DisbursementEnvelopeBatchStatus).where(
                        DisbursementEnvelopeBatchStatus.disbursement_envelope_id
                        == str(disbursements_in_db[0].disbursement_envelope_id)
                    )
                )
            )
            .scalars()
            .first()
        )

        no_of_disbursements_after_this_request = (
            len(disbursement_payloads)
            - disbursement_envelope_batch_status.number_of_disbursements_received
        )
        total_disbursement_amount_after_this_request = (
            sum(
                [
                    disbursement.disbursement_amount
                    for disbursement in disbursements_in_db
                ]
            )
            - disbursement_envelope_batch_status.total_disbursement_amount_received
        )

        if no_of_disbursements_after_this_request < 0:
            raise DisbursementException(
                G2PBridgeErrorCodes.NO_OF_DISBURSEMENTS_LESS_THAN_ZERO,
                disbursement_payloads,
            )

        if total_disbursement_amount_after_this_request < 0:
            raise DisbursementException(
                G2PBridgeErrorCodes.TOTAL_DISBURSEMENT_AMOUNT_LESS_THAN_ZERO,
                disbursement_payloads,
            )

        return True

    async def validate_request_payload(
        self, disbursement_payloads: List[DisbursementPayload]
    ):
        absolutely_no_error = True

        for disbursement_payload in disbursement_payloads:
            disbursement_payload.response_error_codes = []
            if (
                disbursement_payload.disbursement_id is None
                or disbursement_payload.disbursement_id == ""
            ):
                disbursement_payload.response_error_codes.append(
                    G2PBridgeErrorCodes.INVALID_DISBURSEMENT_ID
                )

            if len(disbursement_payload.response_error_codes) > 0:
                absolutely_no_error = False

        return absolutely_no_error
