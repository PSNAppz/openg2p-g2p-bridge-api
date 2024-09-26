import logging

from openg2p_g2p_bridge_models.models import (
    AccountStatement,
    ProcessStatus,
)
from sqlalchemy import and_, select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app, get_engine
from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="mt940_processor_beat_producer")
def mt940_processor_beat_producer():
    _logger.info("Running mt940_processor_beat_producer")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    with session_maker() as session:
        account_statements = (
            session.execute(
                select(AccountStatement).filter(
                    and_(
                        AccountStatement.statement_process_status
                        == ProcessStatus.PENDING,
                        AccountStatement.statement_process_attempts
                        < _config.statement_process_attempts,
                    )
                )
            )
            .scalars()
            .all()
        )

        for statement in account_statements:
            _logger.info(
                f"Sending mt940_processor_worker task for statement_id: {statement.statement_id}"
            )
            celery_app.send_task(
                "mt940_processor_worker",
                args=[statement.statement_id],
                queue="g2p_bridge_celery_worker_tasks",
            )

        _logger.info("Finished mt940_processor_beat_producer")
