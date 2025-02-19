import logging
from logging import INFO
import sys


def setup_logging() -> logging.Logger:
    logging.basicConfig(
        format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
        level=INFO, stream=sys.stderr
    )

    return logging.getLogger(__name__)