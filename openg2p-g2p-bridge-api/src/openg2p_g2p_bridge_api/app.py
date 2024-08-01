# ruff: noqa: E402
import asyncio
import logging

from .config import Settings

_config = Settings.get_config()

from openg2p_fastapi_common.app import Initializer as BaseInitializer
from openg2p_g2p_bridge_models.models import (
    DisbursementEnvelope,
    DisbursementEnvelopeBatchStatus,
    AccountStatement,
    AccountStatementLob,
    DisbursementRecon,
    DisbursementErrorRecon,
)

from .controllers import (
    AccountStatementController,
    DisbursementController,
    DisbursementEnvelopeController,
)
from .services import (
    AccountStatementService,
    DisbursementEnvelopeService,
    DisbursementService,
)

_logger = logging.getLogger(_config.logging_default_logger_name)


class Initializer(BaseInitializer):
    def initialize(self, **kwargs):
        super().initialize()
        DisbursementEnvelopeService()
        DisbursementService()
        AccountStatementService()
        DisbursementEnvelopeController().post_init()
        DisbursementController().post_init()
        AccountStatementController().post_init()

    def migrate_database(self, args):
        super().migrate_database(args)

        async def migrate():
            _logger.info("Migrating database")
            await DisbursementEnvelope.create_migrate()
            await DisbursementEnvelopeBatchStatus.create_migrate()
            await AccountStatement.create_migrate()

        asyncio.run(migrate())
