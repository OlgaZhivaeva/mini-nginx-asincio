import pytest
from proxy.config import UpstreamConfig
from proxy.upstream_pool import UpstreamPool


def test_upstream_pool_round_robin_sequence():
    """Тест: проверка последовательности A -> B -> A."""
    upstreams = [
        UpstreamConfig(host="127.0.0.1", port=9001),
        UpstreamConfig(host="127.0.0.1", port=9002),
    ]
    pool = UpstreamPool(upstreams)

    assert pool.get_next_upstream().port == 9001

    assert pool.get_next_upstream().port == 9002

    assert pool.get_next_upstream().port == 9001


def test_upstream_pool_empty_list_raises_error():
    """Unit-тест: пустой список апстримов вызывает ValueError."""
    with pytest.raises(ValueError, match="Список апстримов не может быть пустым"):
        UpstreamPool([])
