import asyncio
from asyncio import StreamReader

VALID_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}

async def parse_http_request(reader: StreamReader, timeout: float) -> dict:
    start_line = await asyncio.wait_for(reader.readline(), timeout)
    if not start_line:
        raise ConnectionError("Клиент закрыл соединение.")

    start_line = start_line.decode().strip()
    parts = start_line.split()

    if len(parts) != 3:
        raise ValueError(f"Некорректная стартовая строка: {start_line}")

    method, path, version = parts

    if method.upper() not in VALID_METHODS:
        raise ValueError(f"Некорректный HTTP-метод: {method}")

    if not (
            path.startswith("/")
            or path.startswith("http://")
            or path.startswith("https://")
            or path == "*"
    ):
        raise ValueError(f"Некорректный путь в HTTP-запросе: {path}")

    if not version.startswith("HTTP/"):
        raise ValueError(f"Некорректная версия HTTP: {version}")

    headers = {}
    while True:
        line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        if line == b"\r\n":
            break

        if not line:
            raise ConnectionError("Запрос оборван до завершения заголовков")

        header_line = line.decode().strip()
        if ":" in header_line:
            key, value = header_line.split(":", 1)
            headers[key.strip()] = value.strip()

    return {
        "method": method,
        "path": path,
        "version": version,
        "headers": headers,
    }
