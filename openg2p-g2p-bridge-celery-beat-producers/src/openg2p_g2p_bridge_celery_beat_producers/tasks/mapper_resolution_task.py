import logging

from openg2p_g2p_bridge_models.models import (
    MapperResolutionBatchStatus,
    ProcessStatus,
)
from sqlalchemy import and_, select
from sqlalchemy.orm import sessionmaker

from ..app import celery_app, get_engine
from ..config import Settings

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="mapper_resolution_beat_producer")
def mapper_resolution_beat_producer():
    _logger.info("Running mapper_resolution_beat_producer")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    with session_maker() as session:
        mapper_resolution_batch_statuses = (
            session.execute(
                select(MapperResolutionBatchStatus).filter(
                    and_(
                        MapperResolutionBatchStatus.resolution_status
                        == ProcessStatus.PENDING,
                        MapperResolutionBatchStatus.resolution_attempts
                        < _config.mapper_resolve_attempts,
                    )
                )
            )
            .scalars()
            .all()
        )
        for mapper_resolution_batch_status in mapper_resolution_batch_statuses:
            _logger.info(
                f"{mapper_resolution_batch_status.resolution_attempts} / {_config.mapper_resolve_attempts} attempts done")

            _logger.info(
                f"Sending mapper_resolution_worker task for mapper_resolution_batch_id: {mapper_resolution_batch_status.mapper_resolution_batch_id}"
            )
            celery_app.send_task(
                "mapper_resolution_worker",
                args=[mapper_resolution_batch_status.mapper_resolution_batch_id],
                queue="g2p_bridge_celery_worker_tasks",
            )

        _logger.info("Finished mapper_resolution_beat_producer")
