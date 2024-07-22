import uuid

from fastapi import HTTPException
from openg2p_fastapi_common.context import dbengine
from openg2p_fastapi_common.controller import BaseController
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.future import select

from ..models import Account, FundBlock
from ..schemas import BlockFundsRequest, BlockFundsResponse


class BlockFundsController(BaseController):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.router.tags += ["Funds Management"]

        self.router.add_api_route(
            "/block_funds",
            self.block_funds,
            response_model=BlockFundsResponse,
            methods=["POST"],
        )

    async def block_funds(self, request: BlockFundsRequest) -> BlockFundsResponse:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            # Check if the account exists and has sufficient funds
            stmt = select(Account).where(
                (Account.account_number == request.account_no)
                & (Account.account_currency == request.currency)
            )
            result = await session.execute(stmt)
            account_balance = result.scalars().first()

            if not account_balance:
                raise HTTPException(status_code=404, detail="Account not found")
            if account_balance.book_balance < request.amount:
                raise HTTPException(status_code=400, detail="Insufficient funds")

            new_balance = account_balance.book_balance - request.amount
            await session.execute(
                update(Account)
                .where(Account.account_number == request.account_no)
                .values(book_balance=new_balance)
            )

            block_reference_no = str(uuid.uuid4())
            fund_block = FundBlock(
                block_reference_no=block_reference_no,
                account_no=request.account_no,
                amount=request.amount,
                currency=request.currency,
                active=True,
            )
            session.add(fund_block)

            await session.commit()
            return BlockFundsResponse(block_reference_no=block_reference_no)
