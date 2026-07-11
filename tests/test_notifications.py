from unittest.mock import patch, MagicMock
import os
from chem_utils.notifications import notify_teams


@patch.dict(os.environ, {"MS_TEAMS_WEBHOOK_URL": "https://fake.webhook.url"})
@patch("chem_utils.notifications.pymsteams.connectorcard")
def test_notify_teams_sends_card_when_webhook_configured(mock_connectorcard):
    mock_card = MagicMock()
    mock_connectorcard.return_value = mock_card

    notify_teams("Test message", is_error=False)

    mock_connectorcard.assert_called_once_with("https://fake.webhook.url")
    mock_card.text.assert_called_once_with("Test message")
    mock_card.send.assert_called_once()


@patch.dict(os.environ, {}, clear=True)
@patch("chem_utils.notifications.pymsteams.connectorcard")
def test_notify_teams_noop_when_webhook_not_configured(mock_connectorcard):
    notify_teams("Test message", is_error=False)

    mock_connectorcard.assert_not_called()
