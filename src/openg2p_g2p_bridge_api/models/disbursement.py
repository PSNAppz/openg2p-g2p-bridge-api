from datetime import datetime
from enum import Enum

from openg2p_fastapi_common.models import BaseORMModelWithTimes
from sqlalchemy import UUID, DateTime, Float, Integer, String
from sqlalchemy import Enum as SqlEnum
from sqlalchemy.orm import Mapped, mapped_column


class CancellationStatus(Enum):
    NOT_CANCELLED = "NOT_CANCELLED"
    CANCELLED = "CANCELLED"


class ShipmentStatus(Enum):
    PENDING = "PENDING"
    PROCESSED = "PROCESSED"


class ReplyStatus(Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class Disbursement(BaseORMModelWithTimes):
    __tablename__ = "disbursements"
    id = mapped_column(UUID, primary_key=True)
    disbursement_id: Mapped[str] = mapped_column(
        String, unique=True
    )  # TODO: Add unique constraint with composite key
    disbursement_envelope_id: Mapped[str] = mapped_column(String, index=True)
    beneficiary_id: Mapped[int] = mapped_column(Integer)
    beneficiary_name: Mapped[str] = mapped_column(String)
    disbursement_amount: Mapped[float] = mapped_column(Float)
    narrative: Mapped[str] = mapped_column(String)
    receipt_time_stamp: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow
    )
    cancellation_status: Mapped[CancellationStatus] = mapped_column(
        SqlEnum(CancellationStatus), default=CancellationStatus.NOT_CANCELLED
    )
    cancellation_time_stamp: Mapped[datetime] = mapped_column(
        DateTime, nullable=True, default=None
    )


class DisbursementBatchStatus(BaseORMModelWithTimes):
    __tablename__ = "disbursement_batch_statuses"
    id = mapped_column(UUID, primary_key=True)
    disbursement_id: Mapped[str] = mapped_column(String, unique=True)
    disbursement_envelope_id: Mapped[str] = mapped_column(String, index=True)
    shipment_to_bank_status: Mapped[ShipmentStatus] = mapped_column(
        SqlEnum(ShipmentStatus), default=ShipmentStatus.PENDING
    )
    shipment_to_bank_time_stamp: Mapped[datetime] = mapped_column(
        DateTime, nullable=True, default=None
    )
    reply_status_from_bank: Mapped[ReplyStatus] = mapped_column(
        SqlEnum(ReplyStatus), default=ReplyStatus.PENDING
    )
    reply_from_bank_time_stamp: Mapped[datetime] = mapped_column(
        DateTime, nullable=True, default=None
    )
    reply_failure_error_code: Mapped[str] = mapped_column(
        String, nullable=True, default=None
    )
    reply_failure_error_message: Mapped[str] = mapped_column(
        String, nullable=True, default=None
    )
    reply_success_fsp_code: Mapped[str] = mapped_column(
        String, nullable=True, default=None
    )
    reply_success_fa: Mapped[str] = mapped_column(String, nullable=True, default=None)
    mapper_resolved_fa: Mapped[str] = mapped_column(String, nullable=True, default=None)
    mapper_resolved_phone_number: Mapped[str] = mapped_column(
        String, nullable=True, default=None
    )
    mapper_resolved_name: Mapped[str] = mapped_column(
        String, nullable=True, default=None
    )
    mapper_resolved_timestamp: Mapped[datetime] = mapped_column(
        DateTime, nullable=True, default=None
    )
    mapper_resolved_retries: Mapped[int] = mapped_column(
        Integer, nullable=True, default=0
    )
