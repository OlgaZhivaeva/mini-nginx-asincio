import asyncio
import pytest
from proxy.exceptions import HttpRequestError
from proxy.utils.http_parser import parse_http_request


class DummyReader:
    """Эмулятор StreamReader."""

    def __init__(self, data: bytes):
        self.reader = asyncio.StreamReader()
        self.reader.feed_data(data)
        self.reader.feed_eof()

    async def readline(self):
        return await self.reader.readline()


@pytest.mark.asyncio
async def test_parse_valid_request():
    """Тест успешного парсинга валидного HTTP-запроса."""
    raw_data = (
        b"GET http://example.com/hello/name HTTP/1.1\r\n"
        b"Host: 127.0.0.1:8080\r\n"
        b"User-Agent: curl/8.4.0\r\n"
        b"\r\n"
    )
    reader = DummyReader(raw_data)
    request = await parse_http_request(reader, timeout=5.0)

    assert request["method"] == "GET"
    assert request["path"] == "http://example.com/hello/name"
    assert request["version"] == "HTTP/1.1"
    assert request["headers"]["host"] == "127.0.0.1:8080"
    assert request["headers"]["user-agent"] == "curl/8.4.0"


@pytest.mark.asyncio
async def test_early_eof_on_start_line():
    """Тест на ранний EOF (клиент сразу закрыл соединение)."""
    reader = DummyReader(b"")
    with pytest.raises(HttpRequestError, match="Клиент закрыл соединение"):
        await parse_http_request(reader, timeout=5.0)


@pytest.mark.asyncio
async def test_early_eof_during_headers():
    """Тест на ранний EOF (соединение оборвано посреди заголовков без CRLF)."""
    raw_data = (
        b"GET /example.com/hello/name HTTP/1.1\r\n"
        b"Host: example.test\r\n"
    )
    reader = DummyReader(raw_data)
    with pytest.raises(HttpRequestError, match="Запрос оборван до завершения заголовков"):
        await parse_http_request(reader, timeout=5.0)


@pytest.mark.asyncio
async def test_invalid_start_line_format():
    """Тест на некорректную стартовую строку (не 3 элемента)."""
    reader = DummyReader(b"GET /example.com/hello/name\r\n\r\n")
    with pytest.raises(HttpRequestError, match="Некорректная стартовая строка"):
        await parse_http_request(reader, timeout=5.0)


@pytest.mark.asyncio
async def test_invalid_http_method():
    """Тест на недопустимый HTTP-метод."""
    reader = DummyReader(b"INVALID /example.com/hello/name HTTP/1.1\r\n\r\n")
    with pytest.raises(HttpRequestError, match="Некорректный HTTP-метод"):
        await parse_http_request(reader, timeout=5.0)


@pytest.mark.asyncio
async def test_invalid_path():
    """Тест на некорректный путь (не начинается с /)."""
    reader = DummyReader(b"GET example.com/hello/name HTTP/1.1\r\n\r\n")
    with pytest.raises(HttpRequestError, match="Некорректный путь"):
        await parse_http_request(reader, timeout=5.0)


@pytest.mark.asyncio
async def test_invalid_http_version():
    """Тест на некорректную версию HTTP."""
    reader = DummyReader(b"GET /example.com/hello/name/hello FOO/1.0\r\n\r\n")
    with pytest.raises(HttpRequestError, match="Некорректная версия HTTP"):
        await parse_http_request(reader, timeout=5.0)

@pytest.mark.asyncio
async def test_parse_conflict_te_and_cl():
    """Тест: парсер выбрасывает HttpRequestError при одновременном TE и CL."""
    raw_data = (
        b"POST /upload HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"Transfer-Encoding: chunked\r\n"
        b"Content-Length: 10\r\n"
        b"\r\n"
    )
    reader = DummyReader(raw_data)
    with pytest.raises(HttpRequestError, match="запрещено"):
        await parse_http_request(reader, timeout=5.0)


@pytest.mark.asyncio
async def test_parse_invalid_content_length():
    """Тест: парсер выбрасывает HttpRequestError при нечисловом Content-Length."""
    raw_data = (
        b"POST /upload HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"Content-Length: nope\r\n"
        b"\r\n"
    )
    reader = DummyReader(raw_data)
    with pytest.raises(HttpRequestError, match="Некорректное значение Content-Length"):
        await parse_http_request(reader, timeout=5.0)
