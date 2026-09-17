import asyncio

from proxy.config import AppConfig
from proxy.config import UpstreamConfig


class UpstreamPool:
    def __init__(self, config: AppConfig):
        self.config = config
        self.upstreams = config.upstreams
        if not self.upstreams:
            raise ValueError("Список апстримов не может быть пустым!")
        limit = self.config.limits.max_conns_per_upstream
        self.semaphores = {
            (upstream.host, upstream.port): asyncio.Semaphore(limit)
            for upstream in self.upstreams
        }
        self.index = 0


    def get_next_upstream(self) -> UpstreamConfig:
        if self.index >= len(self.upstreams):
            self.index = 0

        upstream = self.upstreams[self.index]

        self.index += 1

        return upstream


    def get_semaphore(self, upstream: UpstreamConfig) -> asyncio.Semaphore:
        return self.semaphores[(upstream.host, upstream.port)]
