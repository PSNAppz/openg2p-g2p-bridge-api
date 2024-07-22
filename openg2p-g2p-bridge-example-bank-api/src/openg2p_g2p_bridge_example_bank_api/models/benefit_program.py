from openg2p_fastapi_common.models import BaseORMModelWithTimes
from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column


class BenefitProgram(BaseORMModelWithTimes):
    __tablename__ = "benefit_programs"
    program_mnemonic: Mapped[str] = mapped_column(String, primary_key=True)
    funding_account_number: Mapped[str] = mapped_column(String, index=True)
    funding_account_currency: Mapped[str] = mapped_column(String)
