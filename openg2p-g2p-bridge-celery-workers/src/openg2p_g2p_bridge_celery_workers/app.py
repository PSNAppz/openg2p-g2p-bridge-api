# ruff: noqa: E402

from .config import Settings

_config = Settings.get_config()

from celery import Celery
from openg2p_fastapi_common.app import Initializer as BaseInitializer
from openg2p_fastapi_common.exception import BaseExceptionHandler
from openg2p_g2p_bridge_bank_connectors.app import (
    Initializer as BankConnectorInitializer,
)
from openg2p_g2pconnect_mapper_lib.app import Initializer as MapperInitializer
from sqlalchemy import create_engine

from .helpers import ResolveHelper


class Initializer(BaseInitializer):
    def initialize(self, **kwargs):
        super().init_logger()
        super().init_app()
        BaseExceptionHandler()

        BankConnectorInitializer()
        MapperInitializer()
        ResolveHelper()


def get_engine():
    if _config.db_datasource:
        db_engine = create_engine(_config.db_datasource)
        return db_engine


celery_app = Celery(
    "g2p_bridge_celery_worker",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
    include=["openg2p_g2p_bridge_celery_workers.tasks"],
)

celery_app.conf.timezone = "UTC"
