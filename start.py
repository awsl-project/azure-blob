import logging

from awsl_blob.awsl_blob import migration

logging.basicConfig(
    format="%(asctime)s: %(levelname)s: %(name)s: %(message)s",
    level=logging.INFO
)

# start_consuming()\

_logger = logging.getLogger(__name__)

while True:
    try:
        migration()
    except Exception as e:
        _logger.exception(e)
