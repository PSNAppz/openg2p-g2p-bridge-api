import enum
import logging
import re
import uuid
from datetime import datetime, timedelta
from typing import List

import jwt
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
_logger = logging.getLogger(_config.logging_default_logger_name)


class FAKeys(enum.Enum):
    account_number = "account_number"
    bank_code = "bank_code"
    branch_code = "branch_code"
    account_type = "account_type"
    mobile_number = "mobile_number"
    mobile_wallet_provider = "mobile_wallet_provider"
    email_address = "email_address"
    email_wallet_provider = "email_wallet_provider"
    fa_type = "fa_type"


class KeyValuePair(BaseModel):
    key: FAKeys
    value: str


class ResolveHelper(BaseService):
    def construct_single_resolve_request(self, id: str) -> SingleResolveRequest:
        _logger.info(f"Constructing single resolve request for ID: {id}")
        single_resolve_request = SingleResolveRequest(
            reference_id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            id=id,
            scope="details",
        )
        _logger.info(f"Constructed single resolve request for ID: {id}")
        return single_resolve_request

    def construct_resolve_request(
        self, single_resolve_requests: List[SingleResolveRequest]
    ) -> ResolveRequest:
        _logger.info(
            f"Constructing resolve request for {len(single_resolve_requests)} single resolve requests"
        )
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
        _logger.info(
            f"Constructed resolve request for {len(single_resolve_requests)} single resolve requests"
        )
        return resolve_request

    async def create_jwt_token(self, payload, expiration_minutes=60):
        private_key = _config.private_key
        headers = {"alg": "RS256", "typ": "JWT"}
        issuer = _config.issuer
        audience = _config.audience
        payload.update(
            {
                "iss": issuer,
                "aud": audience,
                "exp": datetime.utcnow() + timedelta(minutes=expiration_minutes),
            }
        )
        token = jwt.encode(payload, private_key, algorithm="RS256", headers=headers)
        return token

    def _deconstruct(self, value: str, strategy: str) -> List[KeyValuePair]:
        _logger.info(f"Deconstructing ID/FA: {value}")
        regex_res = re.match(strategy, value)
        deconstructed_list = []
        if regex_res:
            regex_res = regex_res.groupdict()
            try:
                deconstructed_list = [
                    KeyValuePair(key=k, value=v) for k, v in regex_res.items()
                ]
            except Exception as e:
                _logger.error(f"Error while deconstructing ID/FA: {e}")
                raise ValueError("Error while deconstructing ID/FA") from e
        _logger.info(f"Deconstructed ID/FA: {value}")
        return deconstructed_list

    def deconstruct_fa(self, fa: str) -> dict:
        _logger.info(f"Deconstructing FA: {fa}")
        deconstruct_strategy = self._get_deconstruct_strategy(fa)
        if deconstruct_strategy:
            deconstructed_pairs = self._deconstruct(fa, deconstruct_strategy)
            deconstructed_fa = {
                pair.key.value: pair.value for pair in deconstructed_pairs
            }
            _logger.info(f"Deconstructed FA: {fa}")
            return deconstructed_fa
        return {}

    def _get_deconstruct_strategy(self, fa: str) -> str:
        _logger.info(f"Getting deconstruction strategy for FA: {fa}")
        if fa.endswith(MapperResolvedFaType.BANK_ACCOUNT.value):
            return _config.bank_fa_deconstruct_strategy
        elif fa.endswith(MapperResolvedFaType.MOBILE_WALLET.value):
            return _config.mobile_wallet_fa_deconstruct_strategy
        elif fa.endswith(MapperResolvedFaType.EMAIL_WALLET.value):
            return _config.email_wallet_fa_deconstruct_strategy
        _logger.info(f"Deconstruction strategy not found for FA: {fa}")
        return ""
