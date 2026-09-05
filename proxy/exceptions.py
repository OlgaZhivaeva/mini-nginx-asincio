class ProxyBaseError(Exception):
    """Базовое исключение для всех ошибок."""
    pass


class HttpRequestError(ProxyBaseError):
    """Клиент прислал некорректный или неполный HTTP-запрос (400 Bad Request)."""
    pass


class UpstreamError(ProxyBaseError):
    """Ошибка при работе с апстримом (502 Bad Gateway)."""
    pass
