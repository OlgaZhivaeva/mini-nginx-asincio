class ProxyBaseError(Exception):
    """Базовое исключение для всех ошибок."""
    pass


class HttpRequestError(ProxyBaseError):
    """Клиент прислал некорректный или неполный HTTP-запрос (400 Bad Request)."""
    pass


class UpstreamError(ProxyBaseError):
    """Ошибка при работе с апстримом (502 Bad Gateway)."""
    pass


class ClientReadTimeoutError(ProxyBaseError):
    """Таймаут передачи клиентом запроса или тела (408 Request Timeout)."""
    pass


class ClientWriteTimeoutError(ProxyBaseError):
    """Таймаут при отправке ответа клиенту."""
    pass


class UpstreamTimeoutError(ProxyBaseError):
    """Базовый таймаут для апстрима (504 Gateway Timeout)."""
    pass


class UpstreamReadTimeoutError(UpstreamTimeoutError):
    """Таймаут чтения ответа от апстрима (504 Gateway Timeout)."""
    pass


class UpstreamWriteTimeoutError(UpstreamTimeoutError):
    """Таймаут отправки запроса на апстрим (504 Gateway Timeout)."""
    pass


class UpstreamConnectTimeoutError(UpstreamTimeoutError):
    """Таймаут подключения к апстриму (504 Gateway Timeout)."""
    pass
