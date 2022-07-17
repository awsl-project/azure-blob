import logging

from awsl_blob.awsl_blob import cleanup

logging.basicConfig(
    format="%(asctime)s: %(levelname)s: %(name)s: %(message)s",
    level=logging.INFO
)


cleanup()
