import asyncio
import pytest

from proxy.config import (
    AppConfig,
    UpstreamConfig,
    TimeoutConfig,
    LimitConfig,
    LoggingConfig,
)
from proxy.proxy_server import ProxyServer


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


def create_test_config(
    upstream_port: int = 9999,
    upstream_host: str = "127.0.0.1",
    upstreams: list = None,
    connect_ms: int = 3000,
    read_ms: int = 1000,
    write_ms: int = 1000,
    total_ms: int = 5000,
    max_client_conns: int = 100,
    max_conns_per_upstream: int = 10,
) -> AppConfig:
    """Создает тестовый конфиг."""
    if upstreams is None:
        upstreams = [UpstreamConfig(host=upstream_host, port=upstream_port)]

    return AppConfig(
        listen="127.0.0.1:8080",
        upstreams=upstreams,
        timeouts=TimeoutConfig(
            connect_ms=connect_ms,
            read_ms=read_ms,
            write_ms=write_ms,
            total_ms=total_ms,
        ),
        limits=LimitConfig(
            max_client_conns=max_client_conns,
            max_conns_per_upstream=max_conns_per_upstream,
        ),
        logging=LoggingConfig(level="info"),
    )


async def run_proxy(client_data: bytes, config: AppConfig) -> bytes:
    """Запускает обработчик прокси и возвращает полученные байты ответа."""
    client_reader = asyncio.StreamReader()
    client_reader.feed_data(client_data)
    client_writer = MockClientWriter()

    server = ProxyServer(config)
    await server.handle_client(client_reader, client_writer)

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


@pytest.mark.asyncio
async def test_client_read_timeout_returns_408():
    """Тест: Медленный клиент (зависли заголовки) приводит к 408 Request Timeout."""
    config = create_test_config(read_ms=100)
    response = await run_proxy(
        b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n", config
    )

    assert b"408 Request Timeout" in response


@pytest.mark.asyncio
async def test_upstream_connect_timeout_returns_504():
    """Тест: Таймаут подключения к апстриму (возвращает 504 Gateway Timeout)."""
    config = create_test_config(
        upstream_host="10.255.255.1", upstream_port=80, connect_ms=100
    )
    response = await run_proxy(
        b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n", config
    )

    assert b"504 Gateway Timeout" in response


@pytest.mark.asyncio
async def test_no_504_after_response_started(unused_tcp_port):
    """Тест: Если ответ апстрима уже начался, при таймауте 504 Gateway Timeout не отправляется."""
    config = create_test_config(unused_tcp_port, read_ms=100)

    async def partial_upstream(reader, writer):
        await reader.readline()
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nPart1")
        await writer.drain()

        await asyncio.sleep(0.5)
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(partial_upstream, "127.0.0.1", unused_tcp_port)

    async with server:
        response = await run_proxy(
            b"GET /slow HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n", config
        )

        assert b"200 OK" in response
        assert b"Part1" in response
        assert b"504 Gateway Timeout" not in response


@pytest.mark.asyncio
async def test_slow_upstream_returns_504(unused_tcp_port):
    """Тест: Медленный апстрим вызывает 504 Gateway Timeout."""
    config = create_test_config(unused_tcp_port, read_ms=100)

    async def slow_upstream(reader, writer):
        await reader.readline()
        await asyncio.sleep(0.5)
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(slow_upstream, "127.0.0.1", unused_tcp_port)

    async with server:
        response = await run_proxy(
            b"GET /slow HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n", config
        )
        assert b"504 Gateway Timeout" in response


@pytest.mark.asyncio
async def test_exceed_total_timeout_returns_504(unused_tcp_port):
    """Тест: Превышение общего таймаута вызывает 504 Gateway Timeout."""
    config = create_test_config(unused_tcp_port, total_ms=100)
    async def exceed_total_timeout(reader, writer):

        while True:
            line = await reader.readline()
            await asyncio.sleep(0.8)
            if line == b"\r\n":
                break
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: close\r\n\r\nHello Upstream"
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(exceed_total_timeout, "127.0.0.1", unused_tcp_port)

    async with server:

        response = await run_proxy(
            b"GET /slow HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n", config
        )
        assert b"504 Gateway Timeout" in response


@pytest.mark.asyncio
async def test_chunk_payload_timeout_returns_408(unused_tcp_port):
    """Тест: Если клиент прислал размер чанка, но задержал payload, возвращается 408 Request Timeout."""
    config = create_test_config(unused_tcp_port, read_ms=100)

    async def handle_upstream(reader, writer):
        while True:
            line = await reader.readline()
            if line == b"\r\n" or not line:
                break
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(
        handle_upstream, "127.0.0.1", unused_tcp_port
    )

    async with server:
        incomplete_chunked_data = (
            b"POST /upload HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"\r\n"
            b"5\r\n"
        )
        response = await run_proxy(incomplete_chunked_data, config)

        assert b"408 Request Timeout" in response
