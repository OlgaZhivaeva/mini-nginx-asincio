import asyncio
import logging
from asyncio import StreamReader, StreamWriter

from proxy.config import AppConfig, UpstreamConfig
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

            upstream_host = self.config.upstreams[0].host
            upstream_port = self.config.upstreams[0].port
            upstream_reader, upstream_writer = await asyncio.open_connection(upstream_host, upstream_port)
            logger.info(f"Подключились к апстриму {upstream_host}:{upstream_port}")

            request_start_line = (f"{request['method']} {request['path']} {request['version']}\r\n").encode()
            upstream_writer.write(request_start_line)

            for key, value in request['headers'].items():
                request_heder = (f"{key}: {value}\r\n").encode()
                upstream_writer.write(request_heder)
            upstream_writer.write(b'\r\n')
            await upstream_writer.drain()

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
            logger.error(f"Ошибка при обработке запроса от {self.peer}: {e}, exc_info=True")
        finally:
            if upstream_writer:
                upstream_writer.close()
                await upstream_writer.wait_closed()
            logger.info(f"Соединение с апстримом {upstream_host}:{upstream_port} закрыто")
            self.client_writer.close()
            await self.client_writer.wait_closed()
