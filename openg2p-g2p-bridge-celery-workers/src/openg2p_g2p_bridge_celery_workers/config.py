from openg2p_fastapi_common.config import Settings as BaseSettings
from pydantic_settings import SettingsConfigDict

from . import __version__


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="g2p_bridge_celery_workers_", env_file=".env", extra="allow"
    )
    openapi_title: str = "OpenG2P G2P Bridge Celery Workers"
    openapi_description: str = """
        Celery workers for OpenG2P G2P Bridge API
        ***********************************
        Further details goes here
        ***********************************
        """
    openapi_version: str = __version__

    db_dbname: str = "openg2p_g2p_bridge_db"
    db_driver: str = "postgresql"

    celery_broker_url: str = "redis://localhost:6379/0"
    celery_backend_url: str = "redis://localhost:6379/0"

    mapper_resolve_api_url: str = ""

    bank_fa_deconstruct_strategy: str = ""
    mobile_wallet_deconstruct_strategy: str = ""
    email_wallet_deconstruct_strategy: str = ""

    private_key: str = ""
    issuer: str = ""
    audience: str = ""
