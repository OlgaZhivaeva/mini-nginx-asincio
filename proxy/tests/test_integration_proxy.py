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
    proxy_port: int = 8080,
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
        listen=f"127.0.0.1:{proxy_port}",
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


@pytest.mark.asyncio
async def test_conflict_te_and_cl_returns_400():
    """Тест: Конфликт Transfer-Encoding и Content-Length возвращает 400 Bad Request."""
    config = create_test_config()
    bad_request = (
        b"POST /upload HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"Transfer-Encoding: chunked\r\n"
        b"Content-Length: 5\r\n\r\n"
        b"5\r\nHello\r\n0\r\n\r\n"
    )
    response = await run_proxy(bad_request, config)
    assert b"400 Bad Request" in response


@pytest.mark.asyncio
async def test_invalid_content_length_returns_400():
    """Тест: Нечисловой Content-Length возвращает 400 Bad Request вместо 500."""
    config = create_test_config()
    bad_request = (
        b"POST /upload HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"Content-Length: nope\r\n\r\n"
    )
    response = await run_proxy(bad_request, config)
    assert b"400 Bad Request" in response


@pytest.mark.asyncio
async def test_chunked_with_trailers(unused_tcp_port):
    """Тест: Chunked запрос с несколькими trailer-полями корректно дочитывается до конца."""
    config = create_test_config(unused_tcp_port)
    received_data = b""

    async def handle_upstream(reader, writer):
        nonlocal received_data
        while True:
            line = await reader.readline()
            received_data += line

            if line == b"\r\n" and b"0\r\n" in received_data:
                break
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle_upstream, "127.0.0.1", unused_tcp_port)

    async with server:
        chunked_with_trailers = (
            b"POST /upload HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n"
            b"5\r\nHello\r\n"
            b"0\r\n"
            b"Expires: Wed, 21 Oct 2026 07:28:00 GMT\r\n"
            b"X-Checksum: abc123xyz\r\n"
            b"\r\n"
        )
        response = await run_proxy(chunked_with_trailers, config)

        assert b"200 OK" in response
        assert b"Expires: Wed, 21 Oct 2026" in received_data
        assert b"X-Checksum: abc123xyz" in received_data


@pytest.mark.asyncio
async def test_early_eof_from_upstream_returns_502(unused_tcp_port):
    """Тест: Ранний EOF от апстрима (закрыл сокет без ответа) возвращает 502 Bad Gateway."""
    config = create_test_config(unused_tcp_port)

    async def silent_close_upstream(reader, writer):
        while True:
            line = await reader.readline()
            if line == b"\r\n" or not line:
                break
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(silent_close_upstream, "127.0.0.1", unused_tcp_port)

    async with server:
        response = await run_proxy(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n", config)
        assert b"502 Bad Gateway" in response


@pytest.mark.asyncio
async def test_chunk_missing_crlf_returns_400(unused_tcp_port):
    """Тест: Чанк с нарушенным форматом (нет CRLF на конце) возвращает 400 Bad Request."""
    config = create_test_config(unused_tcp_port)

    async def handle_upstream(reader, writer):
        await reader.readline()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle_upstream, "127.0.0.1", unused_tcp_port)

    async with server:
        broken_chunk = (
            b"POST /upload HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n"
            b"5\r\nHelloXX0\r\n\r\n"
        )
        response = await run_proxy(broken_chunk, config)
        assert b"400 Bad Request" in response


@pytest.mark.asyncio
async def test_real_tcp_total_timeout_returns_504(unused_tcp_port_factory):
    """Интеграционный тест с реальным TCP-клиентом:
    проверяет, что при total_timeout клиент получает 504 до закрытия сокета.
    """
    proxy_port = unused_tcp_port_factory()
    upstream_port = unused_tcp_port_factory()

    config = create_test_config(
        upstream_port=upstream_port, proxy_port=proxy_port, total_ms=200
    )

    async def slow_upstream(reader, writer):
        while True:
            line = await reader.readline()
            if line == b"\r\n" or not line:
                break
        await asyncio.sleep(1.0)
        writer.close()
        await writer.wait_closed()

    upstream_srv = await asyncio.start_server(
        slow_upstream, "127.0.0.1", upstream_port
    )
    proxy = ProxyServer(config)
    proxy_srv = await asyncio.start_server(
        proxy.handle_client, "127.0.0.1", proxy_port
    )

    async with upstream_srv, proxy_srv:
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        writer.write(b"GET /timeout HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        await writer.drain()

        response = await reader.read(4096)

        writer.close()
        await writer.wait_closed()

        assert b"HTTP/1.1 504 Gateway Timeout" in response
