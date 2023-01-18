
import sys

from loguru import logger

logger.remove()
logger.add(
    sink=sys.stdout,
    level="DEBUG",
    # format="{time:!UTC} {module}: {line} <level>{message}</level>",
    format="{time:!UTC}: <level>{message}</level>",
    colorize=True
)

