# ruff: noqa: E402
import asyncio
import logging

from .config import Settings

_config = Settings.get_config()

from openg2p_fastapi_common.app import Initializer as BaseInitializer

from .controllers import (
    BlockFundsController,
    FundAvailabilityController,
    PaymentController,
)
from .models import Account, BenefitProgram, FundBlock, InitiatePaymentRequest

_logger = logging.getLogger(_config.logging_default_logger_name)


class Initializer(BaseInitializer):
    def initialize(self, **kwargs):
        super().initialize()

        BlockFundsController().post_init()
        FundAvailabilityController().post_init()
        PaymentController().post_init()

    def migrate_database(self, args):
        super().migrate_database(args)

        async def migrate():
            _logger.info("Migrating database")
            await BenefitProgram.create_migrate()
            await Account.create_migrate()
            await FundBlock.create_migrate()
            await InitiatePaymentRequest.create_migrate()

        asyncio.run(migrate())
