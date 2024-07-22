from fastapi import HTTPException
from openg2p_fastapi_common.context import dbengine
from openg2p_fastapi_common.controller import BaseController
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.future import select

from ..models import Account
from ..schemas import CheckFundRequest, CheckFundResponse


class FundAvailabilityController(BaseController):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.router.tags += ["Fund Availability"]

        self.router.add_api_route(
            "/check_funds",
            self.check_available_funds,
            response_model=CheckFundResponse,
            methods=["POST"],
        )

    async def check_available_funds(
        self, request: CheckFundRequest
    ) -> CheckFundResponse:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            stmt = select(Account).where(
                Account.account_number == request.account_number
            )
            result = await session.execute(stmt)
            account_balance = result.scalars().first()

            if not account_balance:
                raise HTTPException(status_code=404, detail="Account not found")

            if account_balance.book_balance >= request.total_funds_needed:
                return CheckFundResponse(
                    account_number=account_balance.account_number,
                    has_sufficient_funds=True,
                )
            else:
                return CheckFundResponse(
                    account_number=account_balance.account_number,
                    has_sufficient_funds=False,
                )
