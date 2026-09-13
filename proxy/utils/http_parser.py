import asyncio
from asyncio import StreamReader

from proxy.exceptions import HttpRequestError

VALID_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}

async def parse_http_request(reader: StreamReader, timeout: float) -> dict:
    """
    Вычитывает из сокета HTTP-запрос, валидирует его синтаксис/фрейминг (RFC 7230)
    и возвращает нормализованный словарь запроса.
    """
    start_line = await asyncio.wait_for(reader.readline(), timeout)
    if not start_line:
        raise HttpRequestError("Клиент закрыл соединение до отправки запроса.")

    start_line = start_line.decode().strip()
    parts = start_line.split()

    if len(parts) != 3:
        raise HttpRequestError(f"Некорректная стартовая строка: {start_line}")

    method, path, version = parts

    if method.upper() not in VALID_METHODS:
        raise HttpRequestError(f"Некорректный HTTP-метод: {method}")

    if not (
            path.startswith("/")
            or path.startswith("http://")
            or path.startswith("https://")
            or path == "*"
    ):
        raise HttpRequestError(f"Некорректный путь в HTTP-запросе: {path}")

    if not version.startswith("HTTP/"):
        raise HttpRequestError(f"Некорректная версия HTTP: {version}")

    headers = {}
    cl_count = 0
    while True:
        line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        if line == b"\r\n":
            break

        if not line:
            raise HttpRequestError("Запрос оборван до завершения заголовков")

        header_line = line.decode().strip()
        if ":" not in header_line:
            raise HttpRequestError(f"Некорректный заголовок: {header_line}")

        key, value = header_line.split(":", 1)
        key = key.strip().lower()
        value = value.strip()

        if key == "content-length":
            cl_count += 1
            if cl_count > 1:
                raise HttpRequestError("Обнаружено несколько заголовков Content-Length")
            headers[key] = value
        else:
            if key in headers:
                headers[key] = f"{headers[key]}, {value}"
            else:
                headers[key] = value

    transfer_encoding = headers.get("transfer-encoding", "").lower()
    has_chunked = "chunked" in [te.strip() for te in transfer_encoding.split(",") if te.strip()]
    has_cl = "content-length" in headers
    content_length = 0

    if has_chunked and has_cl:
        raise HttpRequestError("Одновременное использование Transfer-Encoding и Content-Length запрещено")

    if has_cl:
        cl_value = headers["content-length"]
        if not cl_value.isdigit():
            raise HttpRequestError(f"Некорректное значение Content-Length: {cl_value}")
        content_length = int(cl_value)

    return {
        "method": method.upper(),
        "path": path,
        "version": version,
        "headers": headers,
        "transfer_encoding": "chunked" if has_chunked else "",
        "content_length": content_length,
    }
