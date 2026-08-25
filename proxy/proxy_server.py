import asyncio
import logging
from asyncio import StreamReader, StreamWriter

from proxy.config import AppConfig
from proxy.utils.http_parser import parse_http_request

logger = logging.getLogger(__name__)


class ProxyServer:
    def __init__(self, config: AppConfig):
        self.config = config

    async def handle_client(
        self, client_reader: StreamReader, client_writer: StreamWriter
    ):
        """Обработка входящего клиента."""
        peer = client_writer.get_extra_info("peername")
        logger.info(f"Пришел запрос от {peer[0]}:{peer[1]}")

        try:
            request = await parse_http_request(reader=client_reader)

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
            client_writer.write(response)
            await client_writer.drain()

        except Exception as e:
            logger.error(f"Ошибка при обработке запроса от {peer}: {e}")
        finally:
            client_writer.close()
            await client_writer.wait_closed()

    async def run(self):
        """Запуск TCP-сервера."""
        host, port_str = self.config.listen.split(":")
        port = int(port_str)

        srv = await asyncio.start_server(self.handle_client, host, port)
        logger.info(f"Мини-Nginx запущен на {host}:{port}")

        async with srv:
            await srv.serve_forever()
