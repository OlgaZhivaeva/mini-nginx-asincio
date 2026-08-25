import logging
from asyncio import StreamReader, StreamWriter

from proxy.config import AppConfig
from proxy.utils.http_parser import parse_http_request

logger = logging.getLogger(__name__)


class ClientConnectionHandler:
    def __init__(
            self,
            client_reader: StreamReader,
            client_writer: StreamWriter,
            config: AppConfig
    ):
        self.client_reader = client_reader
        self.client_writer = client_writer
        self.config = config
        self.peer = client_writer.get_extra_info('peername')

    async def handle_connection(self):
        logger.info(f"Пришел запрос от {self.peer[0]}:{self.peer[1]}")
        try:
            request = await parse_http_request(reader=self.client_reader)

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
            self.client_writer.write(response)
            await self.client_writer.drain()

        except Exception as e:
            logger.error(f"Ошибка при обработке запроса от {self.peer}: {e}")
        finally:
            self.client_writer.close()
            await self.client_writer.wait_closed()
