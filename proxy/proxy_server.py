import asyncio
import logging
from asyncio import StreamReader, StreamWriter

from proxy.config import AppConfig
from proxy.client_handler import ClientConnectionHandler

logger = logging.getLogger(__name__)


class ProxyServer:
    def __init__(self, config: AppConfig):
        self.config = config

    async def handle_client(
        self, client_reader: StreamReader, client_writer: StreamWriter
    ):
        """Обработка входящего клиента."""
        handler = ClientConnectionHandler(client_reader, client_writer, self.config)
        handler.peer = client_writer.get_extra_info("peername")

        try:
            await asyncio.wait_for(handler.handle_connection(), timeout=handler.total_timeout)
        except asyncio.TimeoutError:
            if not handler.response_started:
                logger.warning(f"Превышен общий таймаут обработки запроса для клиента {handler.peer}")
                await handler.send_error(b"HTTP/1.1 504 Gateway Timeout", b"504 Gateway Timeout")

    async def run(self):
        """Запуск TCP-сервера."""
        host, port_str = self.config.listen.split(":")
        port = int(port_str)

        srv = await asyncio.start_server(self.handle_client, host, port)
        logger.info(f"Мини-Nginx запущен на {host}:{port}")

        async with srv:
            await srv.serve_forever()
