import asyncio
import logging
import sys

from asyncio import StreamReader, StreamWriter
from config import AppConfig, load_config
from pathlib import Path

from utils.http_parser import parse_http_request
from proxy.proxy_server import ProxyServer

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"

logger = logging.getLogger(__name__)


def setup_logging(level_name: str):
    log_level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


async def main():
    config: AppConfig = load_config(CONFIG_PATH)
    setup_logging(config.logging.level)

    server = ProxyServer(config)
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
