from datetime import datetime
from enum import Enum

from openg2p_fastapi_common.models import BaseORMModelWithTimes
from sqlalchemy import UUID, Boolean, Date, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column


class FundsAvailableWithBankEnum(Enum):
    PENDING_CHECK = "PENDING_CHECK"
    FUNDS_NOT_AVAILABLE = "FUNDS_NOT_AVAILABLE"
    FUNDS_AVAILABLE = "FUNDS_AVAILABLE"


class FundsBlockedWithBankEnum(Enum):
    PENDING_CHECK = "PENDING_CHECK"
    FUNDS_BLOCK_SUCCESS = "FUNDS_BLOCK_SUCCESS"
    FUNDS_BLOCK_FAILURE = "FUNDS_BLOCK_FAILURE"


class DisbursementFrequency(Enum):
    Daily = "Daily"
    Weekly = "Weekly"
    Fortnightly = "Fortnightly"
    Monthly = "Monthly"
    BiMonthly = "BiMonthly"
    Quarterly = "Quarterly"
    SemiAnnually = "SemiAnnually"
    Annually = "Annually"
    OnDemand = "OnDemand"


class CancellationStatus(Enum):
    Not_Canceled = "Not_Canceled"
    Canceled = "Canceled"


class DisbursementEnvelope(BaseORMModelWithTimes):
    __tablename__ = "disbursement_envelopes"
    id = mapped_column(UUID, primary_key=True)
    disbursement_envelope_id: Mapped[str] = mapped_column(String, unique=True)
    benefit_program_mnemonic: Mapped[str] = mapped_column(String)
    disbursement_frequency: Mapped[DisbursementFrequency] = mapped_column(String)
    cycle_code_mnemonic: Mapped[str] = mapped_column(String)
    number_of_beneficiaries: Mapped[int] = mapped_column(Integer)
    number_of_disbursements: Mapped[int] = mapped_column(Integer)
    total_disbursement_amount: Mapped[float] = mapped_column(Integer)
    disbursement_schedule_date: Mapped[datetime.date] = mapped_column(Date())
    receipt_time_stamp: Mapped[datetime] = mapped_column(
        DateTime(), default=datetime.utcnow()
    )
    cancellation_status: Mapped[CancellationStatus] = mapped_column(
        String, default=CancellationStatus.Not_Canceled
    )
    cancellation_timestamp: Mapped[datetime] = mapped_column(
        DateTime(), nullable=True, default=None
    )


class DisbursementEnvelopeBatchStatus(BaseORMModelWithTimes):
    __tablename__ = "disbursement_envelope_batch_statuses"
    id = mapped_column(UUID, primary_key=True)
    disbursement_envelope_id: Mapped[str] = mapped_column(String, unique=True)
    number_of_disbursements_received: Mapped[int] = mapped_column(Integer)
    total_disbursement_amount_received: Mapped[int] = mapped_column(Integer)

    funds_available_with_bank: Mapped[FundsAvailableWithBankEnum] = mapped_column(
        String
    )
    funds_available_latest_timestamp: Mapped[datetime] = mapped_column(
        DateTime(), default=None, nullable=True
    )
    funds_available_latest_error_code: Mapped[str] = mapped_column(
        String, nullable=True
    )
    funds_available_retries: Mapped[int] = mapped_column(Integer, default=0)

    funds_blocked_with_bank: Mapped[FundsBlockedWithBankEnum] = mapped_column(String)
    funds_blocked_latest_timestamp: Mapped[datetime] = mapped_column(
        DateTime(), default=None, nullable=True
    )
    funds_blocked_latest_error_code: Mapped[str] = mapped_column(String, nullable=True)
    funds_blocked_retries: Mapped[int] = mapped_column(Integer, default=0)
    funds_blocked_reference_number: Mapped[str] = mapped_column(String, nullable=True)

    id_mapper_resolution_required: Mapped[bool] = mapped_column(
        Boolean, default=True
    )  # TODO: Business logic to be added

    number_of_disbursements_shipped: Mapped[int] = mapped_column(Integer, default=0)
    number_of_disbursements_successful: Mapped[int] = mapped_column(Integer, default=0)
    number_of_disbursements_failed: Mapped[int] = mapped_column(Integer, default=0)
