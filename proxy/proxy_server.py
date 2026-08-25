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
        await handler.handle_connection()

    async def run(self):
        """Запуск TCP-сервера."""
        host, port_str = self.config.listen.split(":")
        port = int(port_str)

        srv = await asyncio.start_server(self.handle_client, host, port)
        logger.info(f"Мини-Nginx запущен на {host}:{port}")

        async with srv:
            await srv.serve_forever()
