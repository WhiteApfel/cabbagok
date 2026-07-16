import pytest
import cabbagok

pytestmark = pytest.mark.asyncio


async def test_connection_init():
    conn = cabbagok.AmqpConnection()
    assert conn.hosts == [("localhost", 5672)]
