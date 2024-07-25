import pytest
from unittest.mock import patch, Mock
from httpx import HTTPStatusError
from datetime import datetime
from openg2p_g2p_bridge_bank_connectors.bank_connectors import ExampleBankConnector
from openg2p_g2p_bridge_bank_connectors.bank_interface.bank_connector_interface import (
    PaymentPayload,
    PaymentStatus,
)
from openg2p_g2p_bridge_models.models import (
    FundsAvailableWithBankEnum,
    FundsBlockedWithBankEnum,
)




@pytest.fixture
def example_bank_connector():
    return ExampleBankConnector()


def test_check_funds_success(example_bank_connector):
    with patch('httpx.Client.post') as mock_post:
        mock_response = Mock()
        mock_response.json.return_value = {"status": "success"}
        mock_post.return_value = mock_response

        response = example_bank_connector.check_funds("123456", "USD", 100.0)

        assert response.status == FundsAvailableWithBankEnum.FUNDS_AVAILABLE
        assert response.error_code == ""


def test_check_funds_failure(example_bank_connector):
    with patch('httpx.Client.post') as mock_post:
        mock_response = Mock()
        mock_response.json.return_value = {"status": "failure"}
        mock_post.return_value = mock_response

        response = example_bank_connector.check_funds("123456", "USD", 100.0)

        assert response.status == FundsAvailableWithBankEnum.FUNDS_NOT_AVAILABLE
        assert response.error_code == ""


def test_check_funds_http_error(example_bank_connector):
    with patch('httpx.Client.post', side_effect=HTTPStatusError("Error", request=Mock(), response=Mock())) as mock_post:
        response = example_bank_connector.check_funds("123456", "USD", 100.0)

        assert response.status == FundsAvailableWithBankEnum.PENDING_CHECK
        assert response.error_code != ""


def test_block_funds_success(example_bank_connector):
    with patch('httpx.Client.post') as mock_post:
        mock_response = Mock()
        mock_response.json.return_value = {"status": "success", "block_reference_no": "BR123"}
        mock_post.return_value = mock_response

        response = example_bank_connector.block_funds("123456", "USD", 100.0)

        assert response.status == FundsBlockedWithBankEnum.FUNDS_BLOCK_SUCCESS
        assert response.block_reference_no == "BR123"
        assert response.error_code == ""


def test_block_funds_failure(example_bank_connector):
    with patch('httpx.Client.post') as mock_post:
        mock_response = Mock()
        mock_response.json.return_value = {"status": "failure", "error_code": "ERR123"}
        mock_post.return_value = mock_response

        response = example_bank_connector.block_funds("123456", "USD", 100.0)

        assert response.status == FundsBlockedWithBankEnum.FUNDS_BLOCK_FAILURE
        assert response.block_reference_no == ""
        assert response.error_code == "ERR123"


def test_block_funds_http_error(example_bank_connector):
    with patch('httpx.Client.post', side_effect=HTTPStatusError("Error", request=Mock(), response=Mock())) as mock_post:
        response = example_bank_connector.block_funds("123456", "USD", 100.0)

        assert response.status == FundsBlockedWithBankEnum.FUNDS_BLOCK_FAILURE
        assert response.block_reference_no == ""
        assert response.error_code != ""


def test_initiate_payment_success(example_bank_connector):
    with patch('httpx.Client.post') as mock_post:
        mock_response = Mock()
        mock_response.json.return_value = {"status": "success"}
        mock_post.return_value = mock_response

        payment_payload = PaymentPayload(
            remitting_account="123456",
            remitting_account_currency="USD",
            payment_amount=100.0,
            funds_blocked_reference_number="BR123",
            beneficiary_id="BID123",
            payment_date=datetime.now()
        )

        response = example_bank_connector.initiate_payment([payment_payload])

        assert response.status == PaymentStatus.SUCCESS
        assert response.error_code == ""


def test_initiate_payment_failure(example_bank_connector):
    with patch('httpx.Client.post') as mock_post:
        mock_response = Mock()
        mock_response.json.return_value = {"status": "failure", "error_message": "Payment error"}
        mock_post.return_value = mock_response

        payment_payload = PaymentPayload(
            remitting_account="123456",
            remitting_account_currency="USD",
            payment_amount=100.0,
            funds_blocked_reference_number="BR123",
            beneficiary_id="BID123",
            payment_date=datetime.now()
        )

        response = example_bank_connector.initiate_payment([payment_payload])

        assert response.status == PaymentStatus.ERROR
        assert response.error_code == "Payment error"


def test_initiate_payment_http_error(example_bank_connector):
    with patch('httpx.Client.post', side_effect=HTTPStatusError("Error", request=Mock(), response=Mock())) as mock_post:
        payment_payload = PaymentPayload(
            remitting_account="123456",
            remitting_account_currency="USD",
            payment_amount=100.0,
            funds_blocked_reference_number="BR123",
            beneficiary_id="BID123",
            payment_date=datetime.now()
        )

        response = example_bank_connector.initiate_payment([payment_payload])

        assert response.status == PaymentStatus.ERROR
        assert response.error_code != ""
