import logging
from datetime import datetime

from openg2p_g2p_bridge_bank_connectors.bank_connectors import BankConnectorFactory
from openg2p_g2p_bridge_bank_connectors.bank_interface.bank_connector_interface import (
    PaymentPayload,
    PaymentStatus,
    BankConnectorInterface
)
from openg2p_g2p_bridge_models.models import (
    BankDisbursementBatchStatus,
    BenefitProgramConfiguration,
    CancellationStatus,
    Disbursement,
    DisbursementBatchControl,
    DisbursementEnvelope,
    DisbursementEnvelopeBatchStatus,
    ProcessStatus,
    FundsBlockedWithBankEnum,
    MapperResolutionDetails,
)
from sqlalchemy import select, and_
from sqlalchemy.orm import sessionmaker

from ..app import get_engine, celery_app
from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="disburse_funds_from_bank_beat_producer")
def disburse_funds_from_bank_beat_producer():
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
                disburse_funds_from_bank_worker.delay(batch.bank_disbursement_batch_id)


@celery_app.task(name="disburse_funds_from_bank_worker")
def disburse_funds_from_bank_worker(bank_disbursement_batch_id: str):
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        batch_status = (
            session.query(BankDisbursementBatchStatus)
            .filter(
                BankDisbursementBatchStatus.bank_disbursement_batch_id
                == bank_disbursement_batch_id
            )
            .first()
        )

        if not batch_status:
            return

        disbursement_envelope_id = batch_status.disbursement_envelope_id
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

        envelope_batch_status = (
            session.query(DisbursementEnvelopeBatchStatus)
            .filter(
                DisbursementEnvelopeBatchStatus.disbursement_envelope_id
                == disbursement_envelope_id
            )
            .first()
        )

        if not envelope_batch_status:
            return

        disbursement_batch_controls = (
            session.query(DisbursementBatchControl)
            .filter(
                DisbursementBatchControl.bank_disbursement_batch_id
                == bank_disbursement_batch_id
            )
            .all()
        )

        disbursement_ids = [
            control.disbursement_id for control in disbursement_batch_controls
        ]
        disbursements = (
            session.query(Disbursement)
            .filter(Disbursement.disbursement_id.in_(disbursement_ids))
            .all()
        )

        benefit_program_configuration = (
            session.query(BenefitProgramConfiguration)
            .filter(
                BenefitProgramConfiguration.benefit_program_mnemonic
                == envelope.benefit_program_mnemonic
            )
            .first()
        )

        payment_payloads = []

        for disbursement in disbursements:
            mapper_details = (
                session.query(MapperResolutionDetails)
                .filter(
                    MapperResolutionDetails.disbursement_id
                    == disbursement.disbursement_id
                )
                .first()
            )

            payment_payloads.append(
                PaymentPayload(
                    remitting_account=benefit_program_configuration.sponsor_bank_account_number,
                    remitting_account_currency=benefit_program_configuration.sponsor_bank_account_currency,
                    payment_amount=disbursement.disbursement_amount,
                    funds_blocked_reference_number=envelope_batch_status.funds_blocked_reference_number,
                    beneficiary_account=mapper_details.bank_account_number if mapper_details else None,
                    beneficiary_account_currency=benefit_program_configuration.sponsor_bank_account_currency,
                    beneficiary_bank_code=mapper_details.bank_code if mapper_details else None,
                    beneficiary_branch_code=mapper_details.branch_code if mapper_details else None,
                    payment_date=datetime.utcnow(),
                    beneficiary_id=disbursement.beneficiary_id,
                    beneficiary_name=disbursement.beneficiary_name,
                    beneficiary_account_type=mapper_details.mapper_resolved_fa_type,
                    beneficiary_phone_no=mapper_details.mobile_number if mapper_details else None,
                    beneficiary_mobile_wallet_provider=mapper_details.mobile_wallet_provider if mapper_details else None,
                    beneficiary_email_wallet_provider=mapper_details.email_wallet_provider if mapper_details else None,
                    beneficiary_email=mapper_details.email_address if mapper_details else None,
                    benefit_program_mnemonic=envelope.benefit_program_mnemonic,
                    cycle_code_mnemonic=envelope.cycle_code_mnemonic,
                )
            )

        bank_connector: BankConnectorInterface = BankConnectorFactory.get_component().get_bank_connector(
            benefit_program_configuration.sponsor_bank_code
        )

        try:
            payment_response = bank_connector.initiate_payment(payment_payloads)

            if payment_response.status == PaymentStatus.SUCCESS:
                batch_status.disbursement_status = ProcessStatus.PROCESSED.value
                batch_status.latest_error_code = None
            else:
                batch_status.disbursement_status = ProcessStatus.PENDING.value
                batch_status.latest_error_code = payment_response.error_code

            batch_status.disbursement_timestamp = datetime.utcnow()
            batch_status.disbursement_attempts += 1

        except Exception as e:
            batch_status.disbursement_status = ProcessStatus.PENDING.value
            batch_status.disbursement_timestamp = datetime.utcnow()
            batch_status.latest_error_code = str(e)
            batch_status.disbursement_attempts += 1

        session.commit()
