import logging
from datetime import datetime

from openg2p_g2p_bridge_models.models import (
    CancellationStatus,
    DisbursementEnvelope,
    DisbursementEnvelopeBatchStatus,
    FundsAvailableWithBankEnum,
)
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app, get_engine
from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="check_funds_with_bank_beat_producer")
def check_funds_with_bank_beat_producer():
    _logger.info("Checking funds with bank")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    with session_maker() as session:
        envelopes = (
            session.execute(
                select(DisbursementEnvelope)
                .filter(
                    DisbursementEnvelope.disbursement_schedule_date <= datetime.utcnow(),
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
                    DisbursementEnvelope.total_disbursement_amount
                    == DisbursementEnvelopeBatchStatus.total_disbursement_amount_received,
                    or_(
                        and_(
                            DisbursementEnvelopeBatchStatus.funds_available_with_bank
                            == FundsAvailableWithBankEnum.PENDING_CHECK.value,
                            DisbursementEnvelopeBatchStatus.funds_available_attempts
                            < _config.funds_available_check_attempts,
                        ),
                        and_(
                            DisbursementEnvelopeBatchStatus.funds_available_with_bank
                            == FundsAvailableWithBankEnum.FUNDS_NOT_AVAILABLE.value,
                            DisbursementEnvelopeBatchStatus.funds_available_attempts
                            < _config.funds_available_check_attempts,
                        ),
                    ),
                )
            )
            .scalars()
            .all()
        )

        for envelope in envelopes:
            _logger.info(
                f"Sending task to check funds with bank for envelope {envelope.disbursement_envelope_id}"
            )
            celery_app.send_task(
                "check_funds_with_bank_worker",
                args=(envelope.disbursement_envelope_id,),
                queue="g2p_bridge_celery_worker_tasks",
            )

        _logger.info("Checking funds with bank beat tasks push completed")
