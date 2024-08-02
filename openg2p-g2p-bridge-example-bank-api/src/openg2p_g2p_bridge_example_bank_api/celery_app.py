import random
import uuid
from datetime import datetime
from typing import List

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

        failure_logs = []
        for initiate_payment_request in initiate_payment_requests:
            account = (
                session.execute(
                    select(Account).where(
                        Account.account_number
                        == initiate_payment_request.remitting_account
                    )
                )
                .scalars()
                .first()
            )

            fund_block = (
                session.execute(
                    select(FundBlock).where(
                        FundBlock.block_reference_no
                        == initiate_payment_request.funds_blocked_reference_number
                    )
                )
                .scalars()
                .first()
            )

            accounting_log: AccountingLog = construct_accounting_log(
                initiate_payment_request
            )

            update_account(account, initiate_payment_request.payment_amount)
            update_fund_block(fund_block, initiate_payment_request.payment_amount)
            initiate_payment_request.payment_status = PaymentStatus.SUCCESS
            initiate_payment_request.payment_initiate_attempts += 1

            failure_random_number = random.randint(1, 100)
            if failure_random_number <= 30:
                failure_logs.append(accounting_log)

            session.add(accounting_log)
            session.add(fund_block)
            session.add(account)

        # End of loop

        generate_failures(account, failure_logs, fund_block, session)

        session.commit()


def construct_accounting_log(initiate_payment_request: InitiatePaymentRequest):
    return AccountingLog(
        reference_no=str(uuid.uuid4()),
        customer_reference_no=initiate_payment_request.payment_reference_number,
        debit_credit=DebitCreditTypes.DEBIT,
        account_number=initiate_payment_request.remitting_account,
        transaction_amount=initiate_payment_request.payment_amount,
        transaction_date=datetime.utcnow(),
        transaction_currency=initiate_payment_request.remitting_account_currency,
        transaction_code="DBT",
        narrative_1=initiate_payment_request.narrative_1,
        narrative_2=initiate_payment_request.narrative_2,
        narrative_3=initiate_payment_request.narrative_3,
        narrative_4=initiate_payment_request.narrative_4,
        narrative_5=initiate_payment_request.narrative_5,
        narrative_6=initiate_payment_request.narrative_6,
        active=True,
    )


def generate_failures(
    account: Account, failure_logs: List[AccountingLog], fund_block: FundBlock, session
):
    failure_reasons = [
        "ACCOUNT_CLOSED",
        "ACCOUNT_NOT_FOUND",
        "ACCOUNT_DORMANT",
        "ACCOUNT_DECEASED",
    ]
    for failure_log in failure_logs:
        account_log: AccountingLog = AccountingLog(
            reference_no=str(uuid.uuid4()),
            customer_reference_no=failure_log.customer_reference_no,
            debit_credit=failure_log.debit_credit,
            account_number=failure_log.account_number,
            transaction_amount=-failure_log.transaction_amount,
            transaction_date=failure_log.transaction_date,
            transaction_currency=failure_log.transaction_currency,
            transaction_code=failure_log.transaction_code,
            narrative_1=failure_log.narrative_1,
            narrative_2=failure_log.narrative_2,
            narrative_3=failure_log.narrative_3,
            narrative_4=failure_log.narrative_4,
            narrative_5=failure_log.narrative_5,
            narrative_6=random.choice(failure_reasons),
            active=True,
        )
        session.add(account_log)

        update_account(account, account_log.transaction_amount)
        update_fund_block(fund_block, account_log.transaction_amount)


def update_account(account, payment_amount):
    account.book_balance -= payment_amount
    account.blocked_amount -= payment_amount
    account.available_balance = account.book_balance - account.blocked_amount


def update_fund_block(fund_block, payment_amount):
    fund_block.amount_released += payment_amount
