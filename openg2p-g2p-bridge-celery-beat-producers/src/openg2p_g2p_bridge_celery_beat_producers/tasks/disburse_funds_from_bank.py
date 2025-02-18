import logging
from datetime import datetime

from openg2p_g2p_bridge_models.models import (
    BankDisbursementBatchStatus,
    CancellationStatus,
    DisbursementEnvelope,
    DisbursementEnvelopeBatchStatus,
    FundsBlockedWithBankEnum,
    ProcessStatus,
)
from sqlalchemy import and_, literal, select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app, get_engine
from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="disburse_funds_from_bank_beat_producer")
def disburse_funds_from_bank_beat_producer():
    _logger.info("Running disburse_funds_from_bank_beat_producer")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    with session_maker() as session:
        # Check if the disbursement schedule date is today if the configuration is
        # not set to process future disbursement schedules
        date_condition = (
            DisbursementEnvelope.disbursement_schedule_date == datetime.now().date()
            if not _config.process_future_disbursement_schedules
            else literal(True)
        )
        envelopes = (
            session.execute(
                select(DisbursementEnvelope)
                .filter(
                    date_condition,
                    DisbursementEnvelope.cancellation_status
                    == CancellationStatus.Not_Cancelled.value,
                )
                .join(
                    DisbursementEnvelopeBatchStatus,
                    DisbursementEnvelope.disbursement_envelope_id
                    == DisbursementEnvelopeBatchStatus.disbursement_envelope_id,
                )
                .filter(
                    DisbursementEnvelope.number_of_disbursements
                    == DisbursementEnvelopeBatchStatus.number_of_disbursements_received,
                    DisbursementEnvelopeBatchStatus.funds_blocked_with_bank
                    == FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS.value,
                )
            )
            .scalars()
            .all()
        )
        for envelope in envelopes:
            pending_batches = (
                session.execute(
                    select(BankDisbursementBatchStatus).filter(
                        and_(
                            BankDisbursementBatchStatus.disbursement_envelope_id
                            == envelope.disbursement_envelope_id,
                            BankDisbursementBatchStatus.disbursement_status
                            == ProcessStatus.PENDING.value,
                            BankDisbursementBatchStatus.disbursement_attempts
                            < _config.funds_disbursement_attempts,
                        )
                    )
                )
                .scalars()
                .all()
            )

            for batch in pending_batches:
                _logger.info(
                    f"Sending task to disburse funds for batch {batch.bank_disbursement_batch_id}"
                )
                celery_app.send_task(
                    "disburse_funds_from_bank_worker",
                    (batch.bank_disbursement_batch_id,),
                    queue="g2p_bridge_celery_worker_tasks",
                )

            _logger.info(
                f"Sent tasks to disburse funds for {len(pending_batches)} batches"
            )
