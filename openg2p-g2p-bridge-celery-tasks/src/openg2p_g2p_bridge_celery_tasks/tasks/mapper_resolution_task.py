import asyncio
import logging
from datetime import datetime

from openg2p_g2p_bridge_models.models import (
    DisbursementBatchControl,
    MapperResolutionBatchStatus,
    MapperResolutionDetails,
    ProcessStatus,
)
from openg2p_g2pconnect_mapper_lib.client import MapperResolveClient
from openg2p_g2pconnect_mapper_lib.schemas import ResolveRequest
from sqlalchemy import select, and_
from sqlalchemy.orm import sessionmaker

from ..app import get_engine, celery_app
from ..config import Settings
from ..helpers import ResolveHelper

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = get_engine()


@celery_app.task(name="mapper_resolution_beat_producer")
def mapper_resolution_beat_producer():
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        mapper_resolution_batch_statuses = (
            session.execute(
                select(MapperResolutionBatchStatus).filter(
                    and_(
                        MapperResolutionBatchStatus.resolution_status
                        == ProcessStatus.PENDING,
                        MapperResolutionBatchStatus.resolution_attempts
                        < _config.mapper_resolve_attempts,
                    )
                )
            )
            .scalars()
            .all()
        )

        for mapper_resolution_batch_status in mapper_resolution_batch_statuses:
            mapper_resolution_worker.delay(
                mapper_resolution_batch_status.mapper_resolution_batch_id
            )


@celery_app.task(name="mapper_resolution_worker")
def mapper_resolution_worker(mapper_resolution_batch_id: str):
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        disbursement_batch_controls = (
            session.execute(
                select(DisbursementBatchControl).filter(
                    DisbursementBatchControl.mapper_resolution_batch_id
                    == mapper_resolution_batch_id
                )
            )
            .scalars()
            .all()
        )

        beneficiary_disbursement_map = {
            control.beneficiary_id: control.disbursement_id
            for control in disbursement_batch_controls
        }
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            resolve_response, error_msg = loop.run_until_complete(
                make_resolve_request(disbursement_batch_controls)
            )
        finally:
            loop.close()

        if not resolve_response:
            session.query(MapperResolutionBatchStatus).filter(
                MapperResolutionBatchStatus.mapper_resolution_batch_id
                == mapper_resolution_batch_id
            ).update(
                {
                    MapperResolutionBatchStatus.resolution_status: ProcessStatus.PENDING,
                    MapperResolutionBatchStatus.latest_error_code: error_msg,
                    MapperResolutionBatchStatus.resolution_attempts: MapperResolutionBatchStatus.resolution_attempts
                    + 1,
                }
            )
            session.commit()
            return

        process_and_store_resolution(
            mapper_resolution_batch_id, resolve_response, beneficiary_disbursement_map
        )


async def make_resolve_request(disbursement_batch_controls):

    resolve_helper = ResolveHelper.get_component()

    single_resolve_requests = [
        resolve_helper.construct_single_resolve_request(control.beneficiary_id)
        for control in disbursement_batch_controls
    ]
    resolve_request: ResolveRequest = resolve_helper.construct_resolve_request(
        single_resolve_requests
    )
    resolve_client = MapperResolveClient()
    try:
        resolve_response = await resolve_client.resolve_request(
            resolve_request, _config.mapper_resolve_api_url
        )
        return resolve_response, None
    except Exception as e:
        error_msg = f"Failed to resolve the request: {e}"
        return None, error_msg


def process_and_store_resolution(
    mapper_resolution_batch_id, resolve_response, beneficiary_disbursement_map
):
    resolve_helper = ResolveHelper.get_component()
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)
    with session_maker() as session:
        details_list = []
        for single_response in resolve_response.message.resolve_response:
            disbursement_id = beneficiary_disbursement_map.get(single_response.id)
            if disbursement_id and (
                single_response.fa != "" or single_response.fa is not None
            ):
                deconstructed_fa = (
                    resolve_helper.deconstruct_fa(single_response.fa)
                )
                details = MapperResolutionDetails(
                    mapper_resolution_batch_id=mapper_resolution_batch_id,
                    disbursement_id=disbursement_id,
                    beneficiary_id=single_response.id,
                    mapper_resolved_fa=single_response.fa,
                    mapper_resolved_name=single_response.account_provider_info.name
                    if single_response.account_provider_info
                    else None,
                    mapper_resolved_fa_type=deconstructed_fa.get("fa_type"),
                    bank_account_number=deconstructed_fa.get("account_number"),
                    bank_code=deconstructed_fa.get("bank_code"),
                    branch_code=deconstructed_fa.get("branch_code"),
                    mobile_number=deconstructed_fa.get("mobile_number"),
                    mobile_wallet_provider=deconstructed_fa.get(
                        "mobile_wallet_provider"
                    ),
                    email_address=deconstructed_fa.get("email_address"),
                    email_wallet_provider=deconstructed_fa.get("email_wallet_provider"),
                    active=True,
                )
                details_list.append(details)
            else:
                _logger.error(
                    f"Failed to resolve the request for beneficiary: {single_response.id}"
                )
                session.query(MapperResolutionBatchStatus).filter(
                    MapperResolutionBatchStatus.mapper_resolution_batch_id
                    == mapper_resolution_batch_id
                ).update(
                    {
                        MapperResolutionBatchStatus.resolution_status: ProcessStatus.PENDING,
                        MapperResolutionBatchStatus.latest_error_code: f"Failed to resolve the request for beneficiary: {single_response.id}",
                        MapperResolutionBatchStatus.resolution_attempts: MapperResolutionBatchStatus.resolution_attempts
                        + 1,
                    }
                )
                session.commit()
                return

        session.add_all(details_list)
        session.query(MapperResolutionBatchStatus).filter(
            MapperResolutionBatchStatus.mapper_resolution_batch_id
            == mapper_resolution_batch_id
        ).update(
            {
                MapperResolutionBatchStatus.resolution_status: ProcessStatus.PROCESSED,
                MapperResolutionBatchStatus.resolution_time_stamp: datetime.utcnow(),
                MapperResolutionBatchStatus.latest_error_code: None,
                MapperResolutionBatchStatus.resolution_attempts: MapperResolutionBatchStatus.resolution_attempts
                + 1,
            }
        )
        session.commit()
