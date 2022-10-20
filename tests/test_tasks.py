from prefect import flow

from prefect_alert.tasks import (
    goodbye_prefect_alert,
    hello_prefect_alert,
)


def test_hello_prefect_alert():
    @flow
    def test_flow():
        return hello_prefect_alert()

    result = test_flow()
    assert result == "Hello, prefect-alert!"


def goodbye_hello_prefect_alert():
    @flow
    def test_flow():
        return goodbye_prefect_alert()

    result = test_flow()
    assert result == "Goodbye, prefect-alert!"
