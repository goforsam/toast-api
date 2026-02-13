"""Google Secret Manager integration"""

import os
import logging
from google.cloud import secretmanager

from .config import BQ_PROJECT_ID

logger = logging.getLogger(__name__)


def get_secret(secret_id: str) -> str:
    """
    Retrieve secret from Google Secret Manager

    Args:
        secret_id: Secret name (e.g., 'TOAST_CLIENT_ID')

    Returns:
        Secret value as string
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{BQ_PROJECT_ID}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode('UTF-8')
    except Exception as e:
        logger.error(f"Failed to retrieve secret {secret_id}: {str(e)}")
        # Fallback to env vars for development
        return os.environ.get(secret_id, '')
