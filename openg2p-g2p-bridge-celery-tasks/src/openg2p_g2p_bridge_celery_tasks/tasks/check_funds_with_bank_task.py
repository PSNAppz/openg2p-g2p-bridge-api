import logging
from datetime import datetime

from openg2p_g2p_bridge_bank_connectors.bank_connectors import (
    BankConnectorFactory,
)
from openg2p_g2p_bridge_models.models import (
    BenefitProgramConfiguration,
    CancellationStatus,
    DisbursementEnvelope,
    DisbursementEnvelopeBatchStatus,
    FundsAvailableWithBankEnum,
)
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import sessionmaker

from ..app import get_engine, celery_app
from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="check_funds_with_bank_beat_producer")
def check_funds_with_bank_beat_producer():
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        envelopes = (
            session.execute(
                select(DisbursementEnvelope)
                .filter(
                    DisbursementEnvelope.disbursement_schedule_date < datetime.utcnow(),
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
            check_funds_with_bank_worker.delay(envelope.disbursement_envelope_id)


@celery_app.task(name="check_funds_with_bank_worker")
def check_funds_with_bank_worker(disbursement_envelope_id: str):
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        envelope = (
            session.query(DisbursementEnvelope)
            .filter(
                DisbursementEnvelope.disbursement_envelope_id
                == disbursement_envelope_id
            )
            .first()
        )

        if not envelope:
            return

        disbursement_envelope_batch_status = (
            session.query(DisbursementEnvelopeBatchStatus)
            .filter(
                DisbursementEnvelopeBatchStatus.disbursement_envelope_id
                == disbursement_envelope_id
            )
            .first()
        )

        if not disbursement_envelope_batch_status:
            return

        benefit_program_configuration = (
            session.query(BenefitProgramConfiguration)
            .filter(
                BenefitProgramConfiguration.benefit_program_mnemonic
                == envelope.benefit_program_mnemonic
            )
            .first()
        )

        total_funds_needed = envelope.total_disbursement_amount
        bank_connector = BankConnectorFactory.get_component().get_bank_connector(
            benefit_program_configuration.sponsor_bank_code
        )

        try:
            funds_available = (
                bank_connector.check_funds(
                    benefit_program_configuration.sponsor_bank_account_number,
                    benefit_program_configuration.sponsor_bank_account_currency,
                    total_funds_needed,
                ).status
                == FundsAvailableWithBankEnum.FUNDS_AVAILABLE
            )

            if funds_available:
                disbursement_envelope_batch_status.funds_available_with_bank = (
                    FundsAvailableWithBankEnum.FUNDS_AVAILABLE.value
                )
            else:
                disbursement_envelope_batch_status.funds_available_with_bank = (
                    FundsAvailableWithBankEnum.FUNDS_NOT_AVAILABLE.value
                )

            disbursement_envelope_batch_status.funds_available_latest_timestamp = (
                datetime.utcnow()
            )
            disbursement_envelope_batch_status.funds_available_latest_error_code = None
            disbursement_envelope_batch_status.funds_available_attempts += 1

        except Exception as e:
            _logger.error(
                f"Error checking funds with bank for envelope {disbursement_envelope_id}: {e}"
            )
            disbursement_envelope_batch_status.funds_available_with_bank = (
                FundsAvailableWithBankEnum.PENDING_CHECK.value
            )
            disbursement_envelope_batch_status.funds_available_latest_timestamp = (
                datetime.utcnow()
            )
            disbursement_envelope_batch_status.funds_available_latest_error_code = str(
                e
            )
            disbursement_envelope_batch_status.funds_available_attempts += 1

        session.commit()
