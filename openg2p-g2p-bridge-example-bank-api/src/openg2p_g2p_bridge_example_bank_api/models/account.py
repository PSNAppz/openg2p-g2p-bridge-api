from datetime import datetime
from enum import Enum

from openg2p_fastapi_common.models import BaseORMModelWithTimes
from sqlalchemy import DateTime, Float, Integer, String
from sqlalchemy import Enum as SqlEnum
from sqlalchemy.orm import Mapped, mapped_column


class PaymentStatus(Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class Account(BaseORMModelWithTimes):
    __tablename__ = "accounts"
    account_number: Mapped[str] = mapped_column(String)
    account_currency: Mapped[str] = mapped_column(String)
    book_balance: Mapped[float] = mapped_column(Float)


class FundBlock(BaseORMModelWithTimes):
    __tablename__ = "fund_blocks"
    block_reference_no: Mapped[str] = mapped_column(String, index=True, unique=True)
    account_no: Mapped[str] = mapped_column(String)
    currency: Mapped[str] = mapped_column(String)
    amount: Mapped[float] = mapped_column(Float)


class InitiatePaymentRequest(BaseORMModelWithTimes):
    __tablename__ = "initiate_payment_requests"
    remitting_account: Mapped[str] = mapped_column(String, nullable=False)
    remitting_account_currency: Mapped[str] = mapped_column(String, nullable=False)
    payment_amount: Mapped[float] = mapped_column(Float, nullable=False)
    funds_blocked_reference_number: Mapped[str] = mapped_column(String, nullable=False)
    beneficiary_id: Mapped[str] = mapped_column(String, nullable=False)
    beneficiary_name: Mapped[str] = mapped_column(String)
    beneficiary_account: Mapped[str] = mapped_column(String)
    beneficiary_account_currency: Mapped[str] = mapped_column(String)
    beneficiary_account_type: Mapped[str] = mapped_column(String)
    beneficiary_bank_code: Mapped[str] = mapped_column(String)
    beneficiary_branch_code: Mapped[str] = mapped_column(String)
    benefit_program_mnemonic: Mapped[str] = mapped_column(String)
    cycle_code_mnemonic: Mapped[str] = mapped_column(String)
    payment_date: Mapped[datetime] = mapped_column(DateTime(), nullable=False)
    payment_initiate_attempts: Mapped[int] = mapped_column(Integer, default=0)
    payment_status: Mapped[PaymentStatus] = mapped_column(
        SqlEnum(PaymentStatus), default=PaymentStatus.PENDING
    )
