import uuid
from datetime import datetime

from celery import Celery
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from .app import get_engine
from .config import Settings
from .models import (
    Account,
    AccountingLog,
    DebitCreditTypes,
    FundBlock,
    InitiatePaymentRequest,
    PaymentStatus,
)

_config = Settings.get_config()

celery_app = Celery(
    "example_bank_celery_tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
)

celery_app.conf.beat_schedule = {
    "process_payments": {
        "task": "process_payments",
        "schedule": 10,
    }
}

celery_app.conf.timezone = "UTC"
_engine = get_engine()


@celery_app.task(name="process_payments")
def process_payments():
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    with session_maker() as session:
        initiate_payment_requests = (
            session.execute(
                select(InitiatePaymentRequest).where(
                    (InitiatePaymentRequest.payment_status.in_(["PENDING", "FAILED"]))
                    & (
                        InitiatePaymentRequest.payment_initiate_attempts
                        < _config.payment_initiate_attempts
                    )
                )
            )
            .scalars()
            .all()
        )

        for payment_request in initiate_payment_requests:
            account = (
                session.execute(
                    select(Account).where(
                        Account.account_number == payment_request.remitting_account
                    )
                )
                .scalars()
                .first()
            )

            fund_block = (
                session.execute(
                    select(FundBlock).where(
                        FundBlock.block_reference_no
                        == payment_request.funds_blocked_reference_number
                    )
                )
                .scalars()
                .first()
            )

            log = AccountingLog(
                reference_no=str(uuid.uuid4()),
                debit_credit=DebitCreditTypes.DEBIT,
                account_number=payment_request.remitting_account,
                transaction_amount=payment_request.payment_amount,
                transaction_date=datetime.utcnow(),
                transaction_currency=payment_request.remitting_account_currency,
                transaction_code="DBT",
                narrative_1=payment_request.narrative_1,
                narrative_2=payment_request.narrative_2,
                narrative_3=payment_request.narrative_3,
                narrative_4=payment_request.narrative_4,
                narrative_5=payment_request.narrative_5,
                narrative_6=payment_request.narrative_6,
                active=True,
            )

            session.add(log)

            fund_block.amount_released += payment_request.payment_amount

            account.book_balance -= payment_request.payment_amount
            account.blocked_amount -= payment_request.payment_amount
            account.available_balance = account.book_balance - account.blocked_amount
            payment_request.payment_status = PaymentStatus.SUCCESS
            payment_request.payment_initiate_attempts += 1

        session.commit()
