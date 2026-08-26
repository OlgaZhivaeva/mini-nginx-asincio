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
        self.read_timeout = self.config.timeouts.read_ms / 1000
        self.connect_timeout = self.config.timeouts.connect_ms / 1000
        self.write_timeout = self.config.timeouts.write_ms / 1000

    async def _pipe(self, reader: StreamReader, writer: StreamWriter):
        """Перекачивает байты из reader в writer с соблюдением backpressure."""
        CHUNK_SIZE = 8192
        while True:
            try:
                chunk = await asyncio.wait_for(reader.read(CHUNK_SIZE), timeout=self.read_timeout)
            except asyncio.TimeoutError:
                logger.warning(f"Таймаут чтения при стриминге для {self.peer}")
                raise

            if not chunk:
                break
            writer.write(chunk)

            try:
                await asyncio.wait_for(writer.drain(), timeout=self.write_timeout)
            except asyncio.TimeoutError:
                logger.warning(f"Таймаут записи при стриминге для {self.peer}")
                raise

    async def _send_upstream_request(self, upstream_writer: StreamWriter, request: dict):
        """Формирует и отправляет полный HTTP-запрос (заголовки + тело) на апстрим."""
        start_line = (
            f"{request['method']} {request['path']} {request['version']}\r\n"
        ).encode()
        upstream_writer.write(start_line)

        headers = request["headers"]
        for key, value in headers.items():
            header_line = f"{key}: {value}\r\n".encode()
            upstream_writer.write(header_line)

        upstream_writer.write(b"\r\n")

        try:
            await asyncio.wait_for(upstream_writer.drain(), timeout=self.write_timeout)
        except asyncio.TimeoutError:
            logger.warning(f"Таймаут записи на апстрим для {self.peer}")
            raise

        content_length = int(headers.get("Content-Length", 0))
        if content_length > 0 or request["method"] in ("POST", "PUT", "PATCH"):
            await self._pipe(self.client_reader, upstream_writer)

        logger.info("Успешно отправили весь запрос на апстрим")

    async def handle_connection(self):
        logger.info(f"Пришел запрос от {self.peer[0]}:{self.peer[1]}")
        upstream_writer = None
        upstream_host = self.config.upstreams[0].host
        upstream_port = self.config.upstreams[0].port

        try:
            request = await parse_http_request(reader=self.client_reader, timeout=self.read_timeout)

            logger.info(
                f'Метод: {request["method"]}, Путь: {request["path"]}, Версия: {request["version"]}'
            )
            logger.info(f'Заголовки: {request["headers"]}')


            upstream_reader, upstream_writer = await asyncio.wait_for(
                asyncio.open_connection(upstream_host, upstream_port),
                timeout=self.connect_timeout
            )
            logger.info(f"Подключились к апстриму {upstream_host}:{upstream_port}")

            await self._send_upstream_request(upstream_writer=upstream_writer, request=request)

            await self._pipe(upstream_reader, self.client_writer)

        except TimeoutError:
            logger.warning(
                f"Таймаут подключения к апстриму {upstream_host}:{upstream_port} для клиента {self.peer}"
            )
            error_response = (
                b"HTTP/1.1 504 Gateway Timeout\r\n"
                b"Content-Type: text/plain\r\n"
                b"Content-Length: 17\r\n"
                b"Connection: close\r\n\r\n"
                b"504 Gateway Timeout"
            )
            self.client_writer.write(error_response)
            await self.client_writer.drain()

        except Exception as e:
            logger.error(f"Ошибка при обработке запроса от {self.peer}: {e}", exc_info=True)
            error_response = (
                b"HTTP/1.1 502 Bad Gateway\r\n"
                b"Content-Type: text/plain\r\n"
                b"Content-Length: 15\r\n"
                b"Connection: close\r\n\r\n"
                b"502 Bad Gateway"
            )
            self.client_writer.write(error_response)
            await self.client_writer.drain()

        finally:
            if upstream_writer:
                upstream_writer.close()
                await upstream_writer.wait_closed()
                logger.info(f"Соединение с апстримом {upstream_host}:{upstream_port} закрыто")

            self.client_writer.close()
            await self.client_writer.wait_closed()
            logger.info(f"Соединение с клиентом {self.peer[0]}:{self.peer[1]} закрыто")