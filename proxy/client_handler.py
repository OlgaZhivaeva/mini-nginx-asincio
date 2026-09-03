import asyncio
import logging
from asyncio import StreamReader, StreamWriter

from proxy.config import AppConfig
from proxy.exceptions import HttpRequestError, UpstreamError
from proxy.utils.http_parser import parse_http_request

CHUNK_SIZE = 8192

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
        self.response_started = False

    async def _send_error(self, status_line: bytes, body: bytes):
        """Вспомогательный метод для отправки HTTP-ошибок клиенту."""
        if self.response_started:
            return

        response = (
            status_line + b"\r\n"
            b"Content-Type: text/plain\r\n"
            b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n"
            b"Connection: close\r\n\r\n"
            + body
        )
        self.client_writer.write(response)
        await self.client_writer.drain()

    async def _pipe(self, reader: StreamReader, writer: StreamWriter):
        """Перекачивает байты из reader в writer с соблюдением backpressure."""
        while True:
            chunk = await reader.read(CHUNK_SIZE)
            if not chunk:
                break
            writer.write(chunk)
            await writer.drain()

            if not self.response_started:
                self.response_started = True

    async def _pipe_exact(self, reader: StreamReader, writer: StreamWriter, total_bytes: int):
        """Перекачивает ровно total_bytes."""
        remaining = total_bytes
        while remaining > 0:
            to_read = min(CHUNK_SIZE, remaining)
            chunk = await reader.read(to_read)
            if not chunk:
                raise HttpRequestError(
                    "Клиент оборвал соединение до передачи полного тела запроса"
                )
            writer.write(chunk)
            await writer.drain()
            remaining -= len(chunk)

    async def _pipe_chunked(self, reader: StreamReader, writer: StreamWriter):
        """Перекачивает chunked-тело запроса от клиента к апстриму."""
        while True:
            line = await reader.readline()
            if not line:
                raise HttpRequestError(
                    "Соединение оборвано во время чтения chunked-тела"
                )

            writer.write(line)
            await writer.drain()

            hex_size = line.decode().split(";")[0].strip()
            try:
                chunk_size = int(hex_size, 16)
            except ValueError:
                raise HttpRequestError(f"Некорректный размер чанка: {hex_size}")

            if chunk_size == 0:
                trailer = await reader.readline()
                writer.write(trailer)
                await writer.drain()
                break

            chunk_data = await reader.readexactly(chunk_size + 2)
            writer.write(chunk_data)
            await writer.drain()

    async def _send_upstream_request(self, upstream_writer: StreamWriter, request: dict):
        """Формирует и отправляет полный HTTP-запрос (заголовки + тело) на апстрим."""
        start_line = (
            f"{request['method']} {request['path']} {request['version']}\r\n"
        ).encode()
        upstream_writer.write(start_line)

        headers = {key.lower(): value for key, value in request["headers"].items()}
        headers["connection"] = "close"

        for key, value in headers.items():
            header_line = f"{key}: {value}\r\n".encode()
            upstream_writer.write(header_line)

        upstream_writer.write(b"\r\n")
        await upstream_writer.drain()

        transfer_encoding = headers.get("transfer-encoding", "")
        if transfer_encoding == "chunked":
            await self._pipe_chunked(self.client_reader, upstream_writer)

        content_length = int(headers.get("content-length", 0))
        if content_length > 0:
            await self._pipe_exact(self.client_reader, upstream_writer, content_length)

        logger.info("Успешно отправили весь запрос на апстрим")

    async def handle_connection(self):
        logger.info(f"Пришел запрос от {self.peer[0]}:{self.peer[1]}")
        upstream_writer = None
        upstream_host = self.config.upstreams[0].host
        upstream_port = self.config.upstreams[0].port

        try:
            request = await parse_http_request(reader=self.client_reader)

            logger.info(
                f'Метод: {request["method"]}, Путь: {request["path"]}, Версия: {request["version"]}'
            )
            logger.info(f'Заголовки: {request["headers"]}')

            try:
                upstream_reader, upstream_writer = await asyncio.open_connection(upstream_host, upstream_port)
                logger.info(f"Подключились к апстриму {upstream_host}:{upstream_port}")
            except OSError as e:
                raise UpstreamError(f"Ошибка подключения к апстриму: {e}")

            await self._send_upstream_request(upstream_writer=upstream_writer, request=request)

            await self._pipe(upstream_reader, self.client_writer)

        except asyncio.TimeoutError:
            logger.warning(f"Таймаут операции для {self.peer}")
            await self._send_error(b"HTTP/1.1 504 Gateway Timeout", b"504 Gateway Timeout")

        except HttpRequestError as e:
            logger.warning(f"Ошибка в запросе клиента {self.peer}: {e}")
            await self._send_error(b"HTTP/1.1 400 Bad Request", b"400 Bad Request")

        except UpstreamError as e:
            logger.error(f"Ошибка апстрима для {self.peer}: {e}")
            await self._send_error(b"HTTP/1.1 502 Bad Gateway", b"502 Bad Gateway")

        except Exception as e:
            logger.error(f"Непредвиденная ошибка для {self.peer}: {e}", exc_info=True)
            await self._send_error(
                b"HTTP/1.1 500 Internal Server Error", b"500 Internal Server Error"
            )

        finally:
            if upstream_writer:
                upstream_writer.close()
                await upstream_writer.wait_closed()
                logger.info(f"Соединение с апстримом {upstream_host}:{upstream_port} закрыто")

            self.client_writer.close()
            await self.client_writer.wait_closed()
            logger.info(f"Соединение с клиентом {self.peer[0]}:{self.peer[1]} закрыто")
