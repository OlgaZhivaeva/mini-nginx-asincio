import asyncio
from asyncio import StreamReader


async def parse_http_request(reader: StreamReader, timeout: float) -> dict:
    start_line = await asyncio.wait_for(reader.readline(), timeout)
    if not start_line:
        raise ConnectionError("Клиент закрыл соединение.")

    start_line = start_line.decode().strip()
    parts = start_line.split()
    method, path, version = parts[0], parts[1], parts[2]

    headers = {}
    while True:
        line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        if not line or line == b"\r\n":
            break

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
