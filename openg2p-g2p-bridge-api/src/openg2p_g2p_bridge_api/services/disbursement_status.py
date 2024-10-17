import logging
from datetime import datetime
from typing import List

from openg2p_fastapi_common.context import dbengine
from openg2p_fastapi_common.service import BaseService
from openg2p_g2p_bridge_models.errors.exceptions import DisbursementStatusException
from openg2p_g2p_bridge_models.models import (
    DisbursementErrorRecon,
    DisbursementRecon,
)
from openg2p_g2p_bridge_models.schemas import (
    DisbursementEnvelopeStatusResponse,
    DisbursementErrorReconPayload,
    DisbursementReconPayload,
    DisbursementReconRecords,
    DisbursementStatusPayload,
    DisbursementStatusRequest,
    DisbursementStatusResponse,
)
from openg2p_g2pconnect_common_lib.schemas import (
    StatusEnum,
    SyncResponseHeader,
)
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.future import select

from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class DisbursementStatusService(BaseService):
    async def get_disbursement_status_payloads(
        self, disbursement_status_request: DisbursementStatusRequest
    ) -> List[DisbursementStatusPayload]:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            try:
                disbursement_status_payloads = []
                for disbursement_id in disbursement_status_request.message:
                    disbursement_recon_records = (
                        await self.get_disbursement_recon_records(
                            session, disbursement_id
                        )
                    )
                    disbursement_status_payload = DisbursementStatusPayload(
                        disbursement_id=disbursement_id,
                        disbursement_recon_records=disbursement_recon_records,
                    )
                    disbursement_status_payloads.append(disbursement_status_payload)
                return disbursement_status_payloads
            except DisbursementStatusException as e:
                _logger.error("Error in getting disbursement status")
                raise e

    async def get_disbursement_recon_records(
        self, session, disbursement_id: str
    ) -> DisbursementReconRecords:
        disbursement_recon_payloads = []
        disbursement_error_recon_payloads = []

        disbursement_recon_payloads_from_db = (
            (
                await session.execute(
                    select(DisbursementRecon).where(
                        DisbursementRecon.disbursement_id == disbursement_id
                    )
                )
            )
            .scalars()
            .all()
        )

        for disbursement_recon_payload in disbursement_recon_payloads_from_db:
            disbursement_recon_payloads.append(
                DisbursementReconPayload(
                    bank_disbursement_batch_id=disbursement_recon_payload.bank_disbursement_batch_id,
                    disbursement_id=disbursement_recon_payload.disbursement_id,
                    disbursement_envelope_id=disbursement_recon_payload.disbursement_envelope_id,
                    beneficiary_name_from_bank=disbursement_recon_payload.beneficiary_name_from_bank,
                    remittance_reference_number=disbursement_recon_payload.remittance_reference_number,
                    remittance_statement_id=disbursement_recon_payload.remittance_statement_id,
                    remittance_statement_number=disbursement_recon_payload.remittance_statement_number,
                    remittance_statement_sequence=disbursement_recon_payload.remittance_statement_sequence,
                    remittance_entry_sequence=disbursement_recon_payload.remittance_entry_sequence,
                    remittance_entry_date=disbursement_recon_payload.remittance_entry_date,
                    remittance_value_date=disbursement_recon_payload.remittance_value_date,
                    reversal_found=disbursement_recon_payload.reversal_found,
                    reversal_statement_id=disbursement_recon_payload.reversal_statement_id,
                    reversal_statement_number=disbursement_recon_payload.reversal_statement_number,
                    reversal_statement_sequence=disbursement_recon_payload.reversal_statement_sequence,
                    reversal_entry_sequence=disbursement_recon_payload.reversal_entry_sequence,
                    reversal_entry_date=disbursement_recon_payload.reversal_entry_date,
                    reversal_value_date=disbursement_recon_payload.reversal_value_date,
                    reversal_reason=disbursement_recon_payload.reversal_reason,
                )
            )

        disbursement_error_recon_payloads_from_db = (
            (
                await session.execute(
                    select(DisbursementErrorRecon).where(
                        DisbursementErrorRecon.disbursement_id == disbursement_id
                    )
                )
            )
            .scalars()
            .all()
        )

        for (
            disbursement_error_recon_payload
        ) in disbursement_error_recon_payloads_from_db:
            disbursement_error_recon_payloads.append(
                DisbursementErrorReconPayload(
                    statement_id=disbursement_error_recon_payload.statement_id,
                    statement_number=disbursement_error_recon_payload.statement_number,
                    statement_sequence=disbursement_error_recon_payload.statement_sequence,
                    entry_sequence=disbursement_error_recon_payload.entry_sequence,
                    entry_date=disbursement_error_recon_payload.entry_date,
                    value_date=disbursement_error_recon_payload.value_date,
                    error_reason=disbursement_error_recon_payload.error_reason,
                    disbursement_id=disbursement_error_recon_payload.disbursement_id,
                    bank_reference_number=disbursement_error_recon_payload.bank_reference_number,
                )
            )

        disbursement_recon_records = DisbursementReconRecords(
            disbursement_recon_payloads=disbursement_recon_payloads,
            disbursement_error_recon_payloads=disbursement_error_recon_payloads,
        )

        return disbursement_recon_records

    async def construct_disbursement_status_error_response(
        self,
        disbursement_status_request: DisbursementStatusRequest,
        code: str,
    ) -> DisbursementStatusResponse:
        response = DisbursementStatusResponse(
            header=SyncResponseHeader(
                message_id=disbursement_status_request.header.message_id,
                message_ts=datetime.now().isoformat(),
                action=disbursement_status_request.header.action,
                status=StatusEnum.rjct,
                status_reason_message=code,
            ),
            message={},
        )

        return response

    async def construct_disbursement_status_success_response(
        self,
        disbursement_status_request: DisbursementStatusRequest,
        disbursement_status_payloads: List[DisbursementStatusPayload],
    ) -> DisbursementStatusResponse:
        response = DisbursementStatusResponse(
            header=SyncResponseHeader(
                message_id=disbursement_status_request.header.message_id,
                message_ts=datetime.now().isoformat(),
                action=disbursement_status_request.header.action,
                status=StatusEnum.succ,
            ),
            message=disbursement_status_payloads,
        )
        return response
