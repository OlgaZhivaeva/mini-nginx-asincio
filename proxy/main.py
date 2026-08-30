import asyncio
import logging
import sys

from asyncio import StreamReader, StreamWriter
from config import AppConfig, load_config
from pathlib import Path

from utils.http_parser import parse_http_request

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


async def client_connected(reader: StreamReader, writer: StreamWriter):
    peer = writer.get_extra_info("peername")
    logger.info(f"Пришел запрос от {peer[0]}:{peer[1]}")

    try:
        request = await parse_http_request(reader=reader)

        logger.info(
            f'Метод: {request["method"]}, Путь: {request["path"]}, Версия: {request["version"]}'
        )
        logger.info(f'Заголовки: {request["headers"]}')

        response = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain\r\n"
            b"Content-Length: 2\r\n"
            b"Connection: close\r\n\r\n"
            b"OK"
        )
        writer.write(response)
        await writer.drain()

    except Exception as e:
        logger.error(f"Ошибка при обработке запроса от {peer}: {e}")
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    config: AppConfig = load_config(CONFIG_PATH)
    setup_logging(config.logging.level)
    host, port = config.listen.split(":")
    port = int(port)

    srv = await asyncio.start_server(client_connected, host, port)
    logger.info(f"Мини-Nginx запущен на {host}:{port}")

    async with srv:
        await srv.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
