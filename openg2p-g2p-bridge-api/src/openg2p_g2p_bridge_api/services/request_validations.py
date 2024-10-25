from openg2p_fastapi_common.service import BaseService
from openg2p_g2p_bridge_models.errors.exceptions import RequestValidationException
from openg2p_g2pconnect_common_lib.schemas import SyncResponseStatusReasonCodeEnum


class RequestValidation(BaseService):
    def validate_signature(self, is_signature_valid) -> None:
        if not is_signature_valid:
            raise RequestValidationException(
                code=SyncResponseStatusReasonCodeEnum.rjct_jwt_invalid,
                message=SyncResponseStatusReasonCodeEnum.rjct_jwt_invalid,
            )

        return None

    def validate_create_disbursement_envelope_request_header(self, request) -> None:
        if request.header.action != "create_disbursement_envelope":
            raise RequestValidationException(
                code=SyncResponseStatusReasonCodeEnum.rjct_action_not_supported,
                message=SyncResponseStatusReasonCodeEnum.rjct_action_not_supported,
            )
        return None

    def validate_cancel_disbursement_envelope_request_header(self, request) -> None:
        if request.header.action != "cancel_disbursement_envelope":
            raise RequestValidationException(
                code=SyncResponseStatusReasonCodeEnum.rjct_action_not_supported,
                message=SyncResponseStatusReasonCodeEnum.rjct_action_not_supported,
            )
        return None

    def validate_request(self, request) -> None:
        return None
