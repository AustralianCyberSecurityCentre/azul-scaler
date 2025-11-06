"""Test cases for the Burrow wrapper."""

import pytest
from pytest_httpx import HTTPXMock

from azul_scaler import settings
from azul_scaler.burrower import Burrower


async def test_get_clusters(httpx_mock: HTTPXMock):
    settings.settings.burrow_url = "http://non_existent_domain:1234"

    httpx_mock.add_response(
        url="http://non_existent_domain:1234/v3/kafka",
        json={
            "error": False,
            "message": "cluster list returned",
            "clusters": ["local"],
            "request": {"url": "/v3/kafka", "host": "burrow-969584cb5-w98db"},
        },
    )

    burrower = Burrower()

    real_instance = await burrower.get_clusters()

    assert real_instance == ["local"]


async def test_get_consumers(httpx_mock: HTTPXMock):
    settings.settings.burrow_url = "http://non_existent_domain:1234"

    httpx_mock.add_response(
        url="http://non_existent_domain:1234/v3/kafka/notlocal/consumer",
        json={
            "error": True,
            "message": "cluster not found",
            "request": {"url": "/v3/kafka/notlocal/consumer", "host": "burrow-969584cb5-w98db"},
        },
    )
    httpx_mock.add_response(
        url="http://non_existent_domain:1234/v3/kafka/local/consumer",
        json={
            "error": False,
            "message": "consumer list returned",
            "consumers": [
                "dev04-azul-Unbox-2024.10.04-binary-allsrc-sourced.extracted-history-live",
                "dev04-azul-Macros-2024.04.29-binary-allsrc-sourced.extracted-history-live",
            ],
            "request": {"url": "/v3/kafka/local/consumer", "host": "burrow-969584cb5-w98db"},
        },
    )

    burrower = Burrower()

    with pytest.raises(Exception, match="Burrow Error"):
        # Non-existent
        await burrower.get_consumers("notlocal")

    real_instance = await burrower.get_consumers("local")

    assert real_instance == [
        "dev04-azul-Unbox-2024.10.04-binary-allsrc-sourced.extracted-history-live",
        "dev04-azul-Macros-2024.04.29-binary-allsrc-sourced.extracted-history-live",
    ]


async def test_get_consumer_detail(httpx_mock: HTTPXMock):
    settings.settings.burrow_url = "http://non_existent_domain:1234"

    httpx_mock.add_response(
        url="http://non_existent_domain:1234/v3/kafka/local/consumer/blah",
        json={
            "error": True,
            "message": "cluster or consumer not found",
            "request": {"url": "/v3/kafka/local/consumer/blah", "host": "burrow-969584cb5-w98db"},
        },
    )
    httpx_mock.add_response(
        url="http://non_existent_domain:1234/v3/kafka/local/consumer/dev04-azul-Macros-2024.04.29-binary-allsrc-sourced.extracted-history-historic",
        json={
            "error": False,
            "message": "consumer detail returned",
            "topics": {
                "azul.dev04.assemblyline.binary.extracted": [
                    {
                        "offsets": [
                            {"offset": 102020, "timestamp": 1731031724450, "observedAt": 1731031724000, "lag": 0}
                        ],
                        "owner": "/192.168.0.1",
                        "client_id": "sarama",
                        "current-lag": 73,
                    }
                ],
                "azul.dev04.incidents.binary.extracted": [
                    {
                        "offsets": [None, None, None, None, None, None, None, None, None, None],
                        "owner": "/192.168.0.1",
                        "client_id": "sarama",
                        "current-lag": 71,
                    }
                ],
                "azul.dev04.incidents.binary.sourced": [
                    {
                        "offsets": [
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            {"offset": 2, "timestamp": 1731016064151, "observedAt": 1731016064000, "lag": 0},
                        ],
                        "owner": "/192.168.0.1",
                        "client_id": "sarama",
                        "current-lag": 74,
                    }
                ],
            },
            "request": {
                "url": "/v3/kafka/local/consumer/dev04-azul-Macros-2024.04.29-binary-allsrc-sourced.extracted-history-historic",
                "host": "burrow-969584cb5-w98db",
            },
        },
    )

    burrower = Burrower()

    with pytest.raises(Exception, match="Burrow Error"):
        # Non-existent
        await burrower.get_consumer_detail("local", "blah")

    real_instance = await burrower.get_consumer_detail(
        "local", "dev04-azul-Macros-2024.04.29-binary-allsrc-sourced.extracted-history-historic"
    )

    assert len(real_instance.keys()) == 3

    entry = real_instance["azul.dev04.assemblyline.binary.extracted"][0]
    assert entry.current_lag == 73
