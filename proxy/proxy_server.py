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
        self.upstream_pool = UpstreamPool(config)
        self.semaphore = asyncio.Semaphore(self.config.limits.max_client_conns)

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

        async def _handle_connection_with_semaphore():
            """Ожидает доступный слот семафора и запускает обработку соединения."""
            async with self.semaphore:
                await handler.handle_connection()

        try:
            await asyncio.wait_for(_handle_connection_with_semaphore(), timeout=handler.total_timeout)
        except asyncio.TimeoutError:
            if not handler.response_started:
                logger.warning(f"Превышен общий таймаут обработки запроса для клиента {handler.peer}")
                await handler.send_error(b"HTTP/1.1 504 Gateway Timeout", b"504 Gateway Timeout")
        except Exception as e:
            logger.error(f"Непредвиденная ошибка при обработке клиента {handler.peer}: {e}", exc_info=True)
            if not handler.response_started:
                await handler.send_error(
                    b"HTTP/1.1 500 Internal Server Error", b"500 Internal Server Error"
                )
        finally:
            try:
                client_writer.close()
                await client_writer.wait_closed()
                logger.info(f"Соединение с клиентом {handler.peer} закрыто")
            except OSError:
                pass

    async def run(self):
        """Запуск TCP-сервера."""
        host, port_str = self.config.listen.split(":")
        port = int(port_str)

        srv = await asyncio.start_server(self.handle_client, host, port)
        logger.info(f"Мини-Nginx запущен на {host}:{port}")

        async with srv:
            await srv.serve_forever()
