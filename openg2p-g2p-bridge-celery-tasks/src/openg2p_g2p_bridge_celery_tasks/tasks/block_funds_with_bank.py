import logging
from datetime import datetime

from openg2p_g2p_bridge_bank_connectors.bank_connectors import BankConnectorFactory
from openg2p_g2p_bridge_bank_connectors.bank_interface import (
    BankConnectorInterface,
    BlockFundsResponse,
)
from openg2p_g2p_bridge_models.models import (
    BenefitProgramConfiguration,
    CancellationStatus,
    DisbursementEnvelope,
    DisbursementEnvelopeBatchStatus,
    FundsAvailableWithBankEnum,
    FundsBlockedWithBankEnum,
)
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app, get_engine
from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="block_funds_with_bank_beat_producer")
def block_funds_with_bank_beat_producer():
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        envelopes = (
            session.execute(
                select(DisbursementEnvelope)
                .filter(
                    DisbursementEnvelope.disbursement_schedule_date
                    <= datetime.utcnow(),
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
                    DisbursementEnvelopeBatchStatus.funds_available_with_bank
                    == FundsAvailableWithBankEnum.FUNDS_AVAILABLE.value,
                    or_(
                        and_(
                            DisbursementEnvelopeBatchStatus.funds_blocked_with_bank
                            == FundsBlockedWithBankEnum.PENDING_CHECK.value,
                            DisbursementEnvelopeBatchStatus.funds_blocked_attempts
                            < _config.funds_blocked_attempts,
                        ),
                        and_(
                            DisbursementEnvelopeBatchStatus.funds_blocked_with_bank
                            == FundsBlockedWithBankEnum.FUNDS_BLOCK_FAILURE.value,
                            DisbursementEnvelopeBatchStatus.funds_blocked_attempts
                            < _config.funds_blocked_attempts,
                        ),
                    ),
                )
            )
            .scalars()
            .all()
        )

        for envelope in envelopes:
            block_funds_with_bank_worker.delay(envelope.disbursement_envelope_id)


@celery_app.task(name="block_funds_with_bank_worker")
def block_funds_with_bank_worker(disbursement_envelope_id: str):
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

        batch_status = (
            session.query(DisbursementEnvelopeBatchStatus)
            .filter(
                DisbursementEnvelopeBatchStatus.disbursement_envelope_id
                == disbursement_envelope_id
            )
            .first()
        )

        if not batch_status:
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
        bank_connector: BankConnectorInterface = (
            BankConnectorFactory.get_component().get_bank_connector(
                benefit_program_configuration.sponsor_bank_code
            )
        )

        try:
            funds_blocked: BlockFundsResponse = bank_connector.block_funds(
                benefit_program_configuration.sponsor_bank_account_number,
                benefit_program_configuration.sponsor_bank_account_currency,
                total_funds_needed,
            )

            if funds_blocked.status == FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS:
                batch_status.funds_blocked_with_bank = (
                    FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS.value
                )
                batch_status.funds_blocked_reference_number = (
                    funds_blocked.block_reference_no
                )
                batch_status.funds_blocked_latest_error_code = None
            else:
                batch_status.funds_blocked_with_bank = (
                    FundsBlockedWithBankEnum.FUNDS_BLOCK_FAILURE.value
                )
                batch_status.funds_blocked_reference_number = ""
                batch_status.funds_blocked_latest_error_code = funds_blocked.error_code

            batch_status.funds_blocked_latest_timestamp = datetime.utcnow()

            batch_status.funds_blocked_attempts += 1

        except Exception as e:
            _logger.error(f"Error blocking funds with bank: {str(e)}")
            batch_status.funds_blocked_with_bank = (
                FundsBlockedWithBankEnum.PENDING_CHECK.value
            )
            batch_status.funds_blocked_latest_timestamp = datetime.utcnow()
            batch_status.funds_blocked_latest_error_code = funds_blocked.error_code
            batch_status.funds_blocked_attempts += 1
            batch_status.funds_blocked_reference_number = ""

        session.commit()
