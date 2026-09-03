import asyncio
import pytest

from proxy.config import (
    AppConfig,
    UpstreamConfig,
    TimeoutConfig,
    LimitConfig,
    LoggingConfig,
)
from proxy.client_handler import ClientConnectionHandler


class MockClientWriter:
    """Имитатор StreamWriter для сбора ответа, отправленного клиенту."""

    def __init__(self):
        self.buffer = bytearray()
        self.closed = False

    def write(self, data: bytes):
        self.buffer.extend(data)

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass

    def get_extra_info(self, name):
        return ("127.0.0.1", 54321)


def create_test_config(upstream_port: int) -> AppConfig:
    """Создает тестовый конфиг."""
    return AppConfig(
        listen="127.0.0.1:8080",
        upstreams=[UpstreamConfig(host="127.0.0.1", port=upstream_port)],
        timeouts=TimeoutConfig(
            connect_ms=1000, read_ms=1000, write_ms=1000, total_ms=2000
        ),
        limits=LimitConfig(max_client_conns=100, max_conns_per_upstream=10),
        logging=LoggingConfig(level="info"),
    )


async def run_proxy(client_data: bytes, config: AppConfig) -> bytes:
    """Запускает обработчик прокси и возвращает полученные байты ответа."""
    client_reader = asyncio.StreamReader()
    client_reader.feed_data(client_data)
    client_writer = MockClientWriter()

    handler = ClientConnectionHandler(client_reader, client_writer, config)
    await handler.handle_connection()
    return bytes(client_writer.buffer)


@pytest.mark.asyncio
async def test_proxy_get_request(unused_tcp_port):
    """Успешное проксирование GET-запроса."""
    config = create_test_config(unused_tcp_port)

    async def handle_upstream(reader, writer):
        await reader.readline()
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: close\r\n\r\nHello Upstream"
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    upstream_server = await asyncio.start_server(
        handle_upstream, "127.0.0.1", unused_tcp_port
    )

    async with upstream_server:
        response = await run_proxy(
            b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n", config
        )
        assert b"200 OK" in response
        assert b"Hello Upstream" in response


@pytest.mark.asyncio
async def test_proxy_post_request_with_content_length(unused_tcp_port):
    """Тест: POST-запрос с Content-Length не зависает и передает тело."""
    config = create_test_config(unused_tcp_port)
    received_body = b""

    async def handle_upstream(reader, writer):
        nonlocal received_body
        while True:
            line = await reader.readline()
            if line == b"\r\n" or not line:
                break
        received_body = await reader.read(5)
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK"
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    upstream_server = await asyncio.start_server(
        handle_upstream, "127.0.0.1", unused_tcp_port
    )

    async with upstream_server:
        post_data = (
            b"POST /api HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Content-Length: 5\r\n"
            b"\r\n"
            b"HELLO"
        )
        response = await run_proxy(post_data, config)

        assert received_body == b"HELLO"
        assert b"200 OK" in response


@pytest.mark.asyncio
async def test_upstream_unavailable_returns_502(unused_tcp_port):
    """Тест: Недоступный апстрим возвращает 502 Bad Gateway."""
    config = create_test_config(unused_tcp_port)
    response = await run_proxy(
        b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n", config
    )

    assert b"502 Bad Gateway" in response


@pytest.mark.asyncio
async def test_proxy_post_case_insensitive_headers(unused_tcp_port):
    """Тест: POST с заголовками в нижнем регистре (content-length, host)."""
    config = create_test_config(unused_tcp_port)
    received_body = b""

    async def handle_upstream(reader, writer):
        nonlocal received_body
        while True:
            line = await reader.readline()
            if line == b"\r\n" or not line:
                break
        received_body = await reader.read(5)
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK"
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    upstream_server = await asyncio.start_server(
        handle_upstream, "127.0.0.1", unused_tcp_port
    )

    async with upstream_server:
        post_data = (
            b"POST /api HTTP/1.1\r\n"
            b"host: 127.0.0.1\r\n"
            b"content-length: 5\r\n"
            b"connection: keep-alive\r\n"
            b"\r\n"
            b"HELLO"
        )
        response = await run_proxy(post_data, config)

        assert received_body == b"HELLO"
        assert b"200 OK" in response


@pytest.mark.asyncio
async def test_proxy_chunked_request(unused_tcp_port):
    """Тест: Запрос с Transfer-Encoding: chunked передается без ошибок."""
    config = create_test_config(unused_tcp_port)
    received_data = b""

    async def handle_upstream(reader, writer):
        nonlocal received_data
        while True:
            line = await reader.readline()
            received_data += line
            if line == b"0\r\n":
                received_data += await reader.readline()
                break
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK"
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    upstream_server = await asyncio.start_server(
        handle_upstream, "127.0.0.1", unused_tcp_port
    )

    async with upstream_server:
        chunked_data = (
            b"POST /upload HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"\r\n"
            b"5\r\nHello\r\n"
            b"6\r\n World\r\n"
            b"0\r\n\r\n"
        )
        response = await run_proxy(chunked_data, config)

        assert b"Hello" in received_data
        assert b" World" in received_data
        assert b"200 OK" in response


@pytest.mark.asyncio
async def test_broken_client_request_returns_400(unused_tcp_port):
    """Тест: Мусорная стартовая строка от клиента приводит к 400 Bad Request, а не 502."""
    config = create_test_config(unused_tcp_port)
    response = await run_proxy(b"BROKEN\r\n\r\n", config)

    assert b"400 Bad Request" in response
    assert b"502 Bad Gateway" not in response