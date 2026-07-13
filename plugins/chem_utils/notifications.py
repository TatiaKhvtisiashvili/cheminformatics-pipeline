import os
import logging
import pymsteams

logger = logging.getLogger(__name__)


def notify_teams(message: str, is_error: bool = False):
    webhook_url = os.environ.get("MS_TEAMS_WEBHOOK_URL")
    if not webhook_url:
        logger.warning("[notify_teams] No MS_TEAMS_WEBHOOK_URL configured, skipping. Message: %s", message)
        return

    card = pymsteams.connectorcard(webhook_url)
    card.title("Cheminformatics Pipeline" + (" — FAILED" if is_error else " — Success"))
    card.text(message)
    card.send()
    logger.info("[notify_teams] Sent Teams notification: %s", message)
