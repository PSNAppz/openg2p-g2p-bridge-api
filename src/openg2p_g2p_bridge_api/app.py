# ruff: noqa: E402
import asyncio

from .config import Settings

_config = Settings.get_config()

from openg2p_fastapi_common.app import Initializer as BaseInitializer

from .controllers import (
    DisbursementController,
    DisbursementEnvelopeController,
)
from .models import DisbursementEnvelope, DisbursementEnvelopeBatchStatus
from .services import (
    DisbursementEnvelopeService,
    DisbursementService,
)


class Initializer(BaseInitializer):
    def initialize(self, **kwargs):
        super().initialize()

        DisbursementEnvelopeService()
        DisbursementService()
        DisbursementEnvelopeController().post_init()
        DisbursementController().post_init()

    def migrate_database(self, args):
        super().migrate_database(args)

        async def migrate():
            print("Migrating database")
            await DisbursementEnvelope.create_migrate()
            await DisbursementEnvelopeBatchStatus.create_migrate()

        asyncio.run(migrate())
