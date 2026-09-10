import asyncio
import logging
from asyncio import StreamReader, StreamWriter

from proxy.config import AppConfig
from proxy.exceptions import (
    ClientReadTimeoutError,
    ClientWriteTimeoutError,
    HttpRequestError,
    UpstreamConnectTimeoutError,
    UpstreamError,
    UpstreamTimeoutError,
    UpstreamReadTimeoutError,
    UpstreamWriteTimeoutError,
)
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

        self.read_timeout = self.config.timeouts.read_ms / 1000
        self.connect_timeout = self.config.timeouts.connect_ms / 1000
        self.write_timeout = self.config.timeouts.write_ms / 1000
        self.total_timeout = self.config.timeouts.total_ms / 1000

    async def send_error(self, status_line: bytes, body: bytes):
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
        first_read = True
        while True:
            try:
                chunk = await asyncio.wait_for(reader.read(CHUNK_SIZE), timeout=self.read_timeout)
            except asyncio.TimeoutError:
                raise UpstreamReadTimeoutError("Апстрим слишком долго передавал ответ")

            if not chunk:
                if first_read:
                    raise UpstreamError("Апстрим закрыл соединение без ответа (ранний EOF)")
                break

            first_read = False
            writer.write(chunk)
            self.response_started = True

            try:
                await asyncio.wait_for(writer.drain(), timeout=self.write_timeout)
            except asyncio.TimeoutError:
                raise ClientWriteTimeoutError("Таймаут записи ответа клиенту")

    async def _pipe_exact(self, reader: StreamReader, writer: StreamWriter, total_bytes: int):
        """Перекачивает ровно total_bytes."""
        remaining = total_bytes
        while remaining > 0:
            to_read = min(CHUNK_SIZE, remaining)
            try:
                chunk = await asyncio.wait_for(reader.read(to_read), timeout=self.read_timeout)
            except asyncio.TimeoutError:
                raise ClientReadTimeoutError("Клиент слишком долго передавал тело запроса")

            if not chunk:
                raise HttpRequestError(
                    "Клиент оборвал соединение до передачи полного тела запроса"
                )
            writer.write(chunk)
            try:
                await asyncio.wait_for(writer.drain(), timeout=self.write_timeout)
            except asyncio.TimeoutError:
                raise UpstreamWriteTimeoutError("Таймаут записи тела на апстрим")
            remaining -= len(chunk)

    async def _pipe_chunked(self, reader: StreamReader, writer: StreamWriter):
        """Перекачивает chunked-тело запроса от клиента к апстриму."""
        while True:
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=self.read_timeout)
            except asyncio.TimeoutError:
                raise ClientReadTimeoutError("Таймаут чтения chunked-строки клиента")
            if not line:
                raise HttpRequestError(
                    "Соединение оборвано во время чтения chunked-тела"
                )

            writer.write(line)
            try:
                await asyncio.wait_for(writer.drain(), timeout=self.write_timeout)
            except asyncio.TimeoutError:
                raise UpstreamWriteTimeoutError("Таймаут записи чанка на апстрим")

            hex_size = line.decode().split(";")[0].strip()
            try:
                chunk_size = int(hex_size, 16)
            except ValueError:
                raise HttpRequestError(f"Некорректный размер чанка: {hex_size}")

            if chunk_size == 0:
                while True:
                    try:
                        trailer = await asyncio.wait_for(reader.readline(), timeout=self.read_timeout)
                    except asyncio.TimeoutError:
                        raise ClientReadTimeoutError("Таймаут чтения трейлера от клиента")
                    writer.write(trailer)
                    try:
                        await asyncio.wait_for(writer.drain(), timeout=self.write_timeout)
                    except asyncio.TimeoutError:
                        raise UpstreamWriteTimeoutError("Таймаут записи трейлера на апстрим")

                    if trailer == b"\r\n":
                        break

                break

            try:
                chunk_data = await asyncio.wait_for(reader.readexactly(chunk_size + 2), timeout=self.read_timeout)
            except asyncio.TimeoutError:
                raise ClientReadTimeoutError("Таймаут чтения данных чанка от клиента")
            except (asyncio.IncompleteReadError, ConnectionResetError):
                raise HttpRequestError("Клиент закрыл соединение до передачи полного чанка")

            if not chunk_data.endswith(b"\r\n"):
                raise HttpRequestError("Чанк должен завершаться последовательностью CRLF")

            writer.write(chunk_data)
            try:
                await asyncio.wait_for(writer.drain(), timeout=self.write_timeout)
            except asyncio.TimeoutError:
                raise UpstreamWriteTimeoutError("Таймаут записи данных чанка на апстрим")

    async def _send_upstream_request(self, upstream_writer: StreamWriter, request: dict):
        """Формирует и отправляет полный HTTP-запрос (заголовки + тело) на апстрим."""
        start_line = (
            f"{request['method']} {request['path']} {request['version']}\r\n"
        ).encode()
        upstream_writer.write(start_line)

        headers = request["headers"]
        headers["connection"] = "close"

        for key, value in headers.items():
            header_line = f"{key}: {value}\r\n".encode()
            upstream_writer.write(header_line)

        upstream_writer.write(b"\r\n")

        try:
            await asyncio.wait_for(upstream_writer.drain(), timeout=self.write_timeout)
        except asyncio.TimeoutError:
            raise UpstreamWriteTimeoutError("Таймаут записи заголовков на апстрим")

        if request["transfer_encoding"] == "chunked":
            await self._pipe_chunked(self.client_reader, upstream_writer)
        elif request["content_length"] > 0:
            await self._pipe_exact(self.client_reader, upstream_writer, request["content_length"])

        logger.info("Успешно отправили весь запрос на апстрим")

    async def handle_connection(self):
        logger.info(f"Пришел запрос от {self.peer[0]}:{self.peer[1]}")
        upstream_writer = None
        upstream_host = self.config.upstreams[0].host
        upstream_port = self.config.upstreams[0].port

        try:
            try:
                request = await parse_http_request(reader=self.client_reader, timeout=self.read_timeout)
            except asyncio.TimeoutError:
                raise ClientReadTimeoutError("Клиент медленно передаёт заголовки")

            logger.info(
                f'Метод: {request["method"]}, Путь: {request["path"]}, Версия: {request["version"]}'
            )
            logger.info(f'Заголовки: {request["headers"]}')

            try:
                upstream_reader, upstream_writer = await asyncio.wait_for(
                    asyncio.open_connection(upstream_host, upstream_port),
                    timeout=self.connect_timeout,
                )
                logger.info(f"Подключились к апстриму {upstream_host}:{upstream_port}")
            except asyncio.TimeoutError:
                raise UpstreamConnectTimeoutError("Таймаут подключения к апстриму")

            except OSError as e:
                raise UpstreamError(f"Ошибка подключения к апстриму: {e}")

            await self._send_upstream_request(upstream_writer=upstream_writer, request=request)

            await self._pipe(upstream_reader, self.client_writer)

        except ClientReadTimeoutError as e:
            logger.warning(f"Таймаут чтения от клиента {self.peer}: {e}")
            await self.send_error(b"HTTP/1.1 408 Request Timeout", b"408 Request Timeout")

        except UpstreamTimeoutError as e:
            logger.warning(f"Таймаут апстрима для {self.peer}: {e}")
            await self.send_error(b"HTTP/1.1 504 Gateway Timeout", b"504 Gateway Timeout")

        except ClientWriteTimeoutError as e:
            logger.warning(f"Таймаут записи ответа клиенту {self.peer}: {e}")

        except HttpRequestError as e:
            logger.warning(f"Ошибка в запросе клиента {self.peer}: {e}")
            await self.send_error(b"HTTP/1.1 400 Bad Request", b"400 Bad Request")

        except UpstreamError as e:
            logger.error(f"Ошибка апстрима для {self.peer}: {e}")
            await self.send_error(b"HTTP/1.1 502 Bad Gateway", b"502 Bad Gateway")

        except Exception as e:
            logger.error(f"Непредвиденная ошибка для {self.peer}: {e}", exc_info=True)
            await self.send_error(
                b"HTTP/1.1 500 Internal Server Error", b"500 Internal Server Error"
            )

        finally:
            if upstream_writer:
                try:
                    upstream_writer.close()
                    await upstream_writer.wait_closed()
                    logger.info(f"Соединение с апстримом {upstream_host}:{upstream_port} закрыто")
                except OSError as e:
                    logger.debug(f"Ошибка при закрытии сокета апстрима: {e}")
