import pytest
from proxy.config import UpstreamConfig
from proxy.tests.test_integration_proxy import create_test_config
from proxy.upstream_pool import UpstreamPool


def test_upstream_pool_round_robin_sequence():
    """Тест: проверка последовательности A -> B -> A."""
    upstreams = [
        UpstreamConfig(host="127.0.0.1", port=9001),
        UpstreamConfig(host="127.0.0.1", port=9002),
    ]
    config = create_test_config(upstreams=upstreams)
    pool = UpstreamPool(config)

    assert pool.get_next_upstream().port == 9001

    assert pool.get_next_upstream().port == 9002

    assert pool.get_next_upstream().port == 9001


def test_upstream_pool_empty_list_raises_error():
    """Тест: пустой список апстримов вызывает ValueError."""
    config = create_test_config(upstreams=[])
    with pytest.raises(ValueError, match="Список апстримов не может быть пустым"):
        UpstreamPool(config)


def test_upstream_pool_creates_semaphores_with_limit():
    """Тест: проверяет создание семафоров с лимитом из конфига."""
    upstreams = [UpstreamConfig(host="127.0.0.1", port=9001)]
    config = create_test_config(upstreams=upstreams, max_conns_per_upstream=3)
    pool = UpstreamPool(config)

    sem = pool.get_semaphore(upstreams[0])
    assert sem._value == 3
