import logging
from datetime import datetime

from openg2p_g2p_bridge_bank_connectors.bank_connectors import BankConnectorFactory
from openg2p_g2p_bridge_bank_connectors.bank_interface.bank_connector_interface import (
    BankConnectorInterface,
    DisbursementPaymentPayload,
    PaymentStatus,
)
from openg2p_g2p_bridge_models.models import (
    BankDisbursementBatchStatus,
    BenefitProgramConfiguration,
    Disbursement,
    DisbursementBatchControl,
    DisbursementEnvelope,
    DisbursementEnvelopeBatchStatus,
    MapperResolutionDetails,
    ProcessStatus,
)
from sqlalchemy.orm import sessionmaker

from ..app import celery_app, get_engine
from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="disburse_funds_from_bank_worker")
def disburse_funds_from_bank_worker(bank_disbursement_batch_id: str):
    _logger.info(f"Disbursing funds with bank for batch: {bank_disbursement_batch_id}")
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
            _logger.error(
                f"Bank Disbursement Batch Status not found for batch: {bank_disbursement_batch_id}"
            )
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
            _logger.error(
                f"Disbursement Envelope not found for envelope: {disbursement_envelope_id}"
            )
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
            _logger.error(
                f"Disbursement Envelope Batch Status not found for envelope: {disbursement_envelope_id}"
            )
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
                DisbursementPaymentPayload(
                    disbursement_id=disbursement.disbursement_id,
                    remitting_account=benefit_program_configuration.sponsor_bank_account_number,
                    remitting_account_currency=benefit_program_configuration.sponsor_bank_account_currency,
                    payment_amount=disbursement.disbursement_amount,
                    funds_blocked_reference_number=envelope_batch_status.funds_blocked_reference_number,
                    beneficiary_account=mapper_details.bank_account_number
                    if mapper_details
                    else None,
                    beneficiary_account_currency=benefit_program_configuration.sponsor_bank_account_currency,
                    beneficiary_bank_code=mapper_details.bank_code
                    if mapper_details
                    else None,
                    beneficiary_branch_code=mapper_details.branch_code
                    if mapper_details
                    else None,
                    payment_date=str(datetime.date(datetime.utcnow())),
                    beneficiary_id=disbursement.beneficiary_id,
                    beneficiary_name=disbursement.beneficiary_name,
                    beneficiary_account_type=mapper_details.mapper_resolved_fa_type,
                    beneficiary_phone_no=mapper_details.mobile_number
                    if mapper_details
                    else None,
                    beneficiary_mobile_wallet_provider=mapper_details.mobile_wallet_provider
                    if mapper_details
                    else None,
                    beneficiary_email_wallet_provider=mapper_details.email_wallet_provider
                    if mapper_details
                    else None,
                    beneficiary_email=mapper_details.email_address
                    if mapper_details
                    else None,
                    disbursement_narrative=disbursement.narrative,
                    benefit_program_mnemonic=envelope.benefit_program_mnemonic,
                    cycle_code_mnemonic=envelope.cycle_code_mnemonic,
                )
            )

        bank_connector: BankConnectorInterface = (
            BankConnectorFactory.get_component().get_bank_connector(
                benefit_program_configuration.sponsor_bank_code
            )
        )

        try:
            payment_response = bank_connector.initiate_payment(payment_payloads)

            if payment_response.status == PaymentStatus.SUCCESS:
                batch_status.disbursement_status = ProcessStatus.PROCESSED.value
                batch_status.latest_error_code = None
                envelope_batch_status.number_of_disbursements_shipped += len(
                    payment_payloads
                )
            else:
                batch_status.disbursement_status = ProcessStatus.PENDING.value
                batch_status.latest_error_code = payment_response.error_code

            batch_status.disbursement_timestamp = datetime.utcnow()
            batch_status.disbursement_attempts += 1

        except Exception as e:
            _logger.error(f"Error disbursing funds with bank: {str(e)}")
            batch_status.disbursement_status = ProcessStatus.PENDING.value
            batch_status.disbursement_timestamp = datetime.utcnow()
            batch_status.latest_error_code = str(e)
            batch_status.disbursement_attempts += 1

        _logger.info(
            f"Disbursing funds with bank for batch: {bank_disbursement_batch_id} completed"
        )
        session.commit()
