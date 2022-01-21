import logging

from awsl_blob.awsl_blob import start_consuming

logging.basicConfig(
    format="%(asctime)s: %(levelname)s: %(name)s: %(message)s",
    level=logging.INFO
)

start_consuming()
