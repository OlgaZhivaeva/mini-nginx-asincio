from proxy.config import UpstreamConfig


class UpstreamPool:
    def __init__(self, upstreams: list[UpstreamConfig]):
        if not upstreams:
            raise ValueError("Список апстримов не может быть пустым!")
        self.upstreams = upstreams
        self.index = 0

    def get_next_upstream(self) -> UpstreamConfig:
        if self.index >= len(self.upstreams):
            self.index = 0

        upstream = self.upstreams[self.index]

        self.index += 1

        return upstream
