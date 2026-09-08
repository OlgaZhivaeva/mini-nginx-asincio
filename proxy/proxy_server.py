import asyncio
import logging
from asyncio import StreamReader, StreamWriter

from proxy.config import AppConfig
from proxy.client_handler import ClientConnectionHandler
from proxy.upstream_pool import UpstreamPool

logger = logging.getLogger(__name__)


class ProxyServer:
    def __init__(self, config: AppConfig):
        self.config = config
        self.upstream_pool = UpstreamPool(config.upstreams)

    async def handle_client(
        self, client_reader: StreamReader, client_writer: StreamWriter
    ):
        """Обработка входящего клиента."""
        handler = ClientConnectionHandler(
            client_reader,
            client_writer,
            self.config,
            self.upstream_pool,
        )
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
