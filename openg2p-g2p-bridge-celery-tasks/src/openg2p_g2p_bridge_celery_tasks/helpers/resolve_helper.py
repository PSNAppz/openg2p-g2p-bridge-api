import enum
import re
import uuid
from datetime import datetime
from typing import List

from openg2p_fastapi_common.service import BaseService
from openg2p_g2p_bridge_models.models import MapperResolvedFaType
from openg2p_g2pconnect_common_lib.schemas import RequestHeader
from openg2p_g2pconnect_mapper_lib.schemas import (
    ResolveRequest,
    ResolveRequestMessage,
    SingleResolveRequest,
)
from pydantic import BaseModel

from ..config import Settings

_config = Settings.get_config()


class FAKeys(enum.Enum):
    account_number = "account_number"
    bank_code = "bank_code"
    branch_code = "branch_code"
    account_type = "account_type"
    mobile_number = "mobile_number"
    mobile_wallet_provider = "mobile_wallet_provider"
    email_address = "email_address"
    email_wallet_provider = "email_wallet_provider"


class KeyValuePair(BaseModel):
    key: FAKeys
    value: str


class ResolveHelper(BaseService):
    def construct_single_resolve_request(self, id: str) -> SingleResolveRequest:
        single_resolve_request = SingleResolveRequest(
            reference_id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            id=id,
            scope="details",
        )
        return single_resolve_request

    def construct_resolve_request(
        self, single_resolve_requests: List[SingleResolveRequest]
    ) -> ResolveRequest:
        resolve_request_message = ResolveRequestMessage(
            transaction_id=str(uuid.uuid4()),
            resolve_request=single_resolve_requests,
        )

        resolve_request = ResolveRequest(
            signature="",
            header=RequestHeader(
                message_id=str(uuid.uuid4()),
                message_ts=str(datetime.now()),
                action="resolve",
                sender_id="",
                sender_uri="",
                total_count=len(single_resolve_requests),
            ),
            message=resolve_request_message,
        )

        return resolve_request

    def _deconstruct(self, value: str, strategy: str) -> List[KeyValuePair]:
        regex_res = re.match(strategy, value)
        deconstructed_list = []
        if regex_res:
            regex_res = regex_res.groupdict()
            try:
                deconstructed_list = [
                    KeyValuePair(key=k, value=v) for k, v in regex_res.items()
                ]
            except Exception as e:
                raise ValueError("Error while deconstructing ID/FA") from e
        return deconstructed_list

    def deconstruct_fa(self, fa: str) -> dict:
        deconstruct_strategy = self.get_deconstruct_strategy(fa)
        if deconstruct_strategy:
            deconstructed_pairs = self._deconstruct(fa, deconstruct_strategy)
            deconstructed_fa = {pair.key: pair.value for pair in deconstructed_pairs}
            return deconstructed_fa
        return {}

    def get_deconstruct_strategy(self, fa: str) -> str:
        if fa.startswith(MapperResolvedFaType.BANK_ACCOUNT.value):
            return _config.bank_fa_deconstruct_strategy
        elif fa.startswith(MapperResolvedFaType.MOBILE_WALLET.value):
            return _config.mobile_wallet_fa_deconstruct_strategy
        elif fa.startswith(MapperResolvedFaType.EMAIL_WALLET.value):
            return _config.email_wallet_fa_deconstruct_strategy
        return ""
