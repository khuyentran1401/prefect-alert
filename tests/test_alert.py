import asyncio
from unittest.mock import AsyncMock, patch

import prefect
from prefect import flow, task

from prefect_alert import alert_on_failure


def create_block(block_type, block_name):
    block = block_type(url="https://example.com/notification")
    block.save(name=block_name, overwrite=True)


class TestAlertFlow:
    def test_sync_success(self):
        with patch("prefect.blocks.notifications.SlackWebhook") as SlackWebhookMock:
            block_type = SlackWebhookMock
            block_name = "test"
            create_block(block_type, block_name)

            @alert_on_failure(block_type=SlackWebhookMock, block_name=block_name)
            @flow
            def succeed_flow():
                return 1

            succeed_flow()
            block_type.load.assert_called_once_with(block_name)

    def test_async_success(self):
        with patch("prefect.blocks.notifications.SlackWebhook") as SlackWebhookMock:
            block_type = SlackWebhookMock
            block_name = "test"
            SlackWebhookMock.load = AsyncMock()
            create_block(block_type, block_name)

            @task
            async def success_task():
                return 1

            @alert_on_failure(block_type=SlackWebhookMock, block_name=block_name)
            @flow
            async def succeed_flow():
                res = await success_task()
                return res

            state = asyncio.run(succeed_flow(return_state=True))
            block_type.load.assert_called_once_with(block_name)
            assert isinstance(state, prefect.client.schemas.State)

    def test_sync_failed(self):
        with patch("prefect.blocks.notifications.SlackWebhook") as SlackWebhookMock:
            block_type = SlackWebhookMock
            block_name = "test"
            block_instance = SlackWebhookMock.load.return_value
            create_block(block_type, block_name)

            @task
            def may_fail():
                raise ValueError()

            @alert_on_failure(block_type=SlackWebhookMock, block_name=block_name)
            @flow
            def failed_flow():
                res = may_fail()
                return res

            state = failed_flow(return_state=True)
            assert isinstance(state, prefect.client.schemas.State)
            block_type.load.assert_called_once_with(block_name)
            block_instance.notify.assert_called_once()

    def test_async_failed(self):
        with patch("prefect.blocks.notifications.SlackWebhook") as SlackWebhookMock:
            block_type = SlackWebhookMock
            block_name = "test"
            SlackWebhookMock.load = AsyncMock()
            block_instance = SlackWebhookMock.load.return_value
            create_block(block_type, block_name)

            @task
            async def may_fail():
                raise ValueError()

            @alert_on_failure(block_type=SlackWebhookMock, block_name=block_name)
            @flow
            async def failed_flow():
                res = await may_fail()
                return res

            state = asyncio.run(failed_flow(return_state=True))
            assert isinstance(state, prefect.client.schemas.State)
            block_type.load.assert_called_once_with(block_name)
            block_instance.notify.assert_called_once()

    def test_return_results(self):
        with patch("prefect.blocks.notifications.SlackWebhook") as SlackWebhookMock:
            block_type = SlackWebhookMock
            block_name = "test"
            create_block(block_type, block_name)

            @alert_on_failure(block_type=SlackWebhookMock, block_name=block_name)
            @flow
            def succeed_flow():
                return 1

            res = succeed_flow()
            assert isinstance(res, int)

    def test_sync_failed_submit(self):
        with patch("prefect.blocks.notifications.SlackWebhook") as SlackWebhookMock:
            block_type = SlackWebhookMock
            block_name = "test"
            block_instance = SlackWebhookMock.load.return_value
            create_block(block_type, block_name)

            @task
            def may_fail():
                raise ValueError()

            @alert_on_failure(block_type=SlackWebhookMock, block_name=block_name)
            @flow
            def failed_flow():
                res = may_fail.submit()
                return res

            state = failed_flow(return_state=True)
            assert isinstance(state, prefect.client.schemas.State)
            block_type.load.assert_called_once_with(block_name)
            block_instance.notify.assert_called_once()

    def test_is_flow_instance(self):
        with patch("prefect.blocks.notifications.SlackWebhook") as SlackWebhookMock:
            # the decorated flow should be an instance of prefect.Flow
            # to ensure it works with deployments
            @alert_on_failure(block_type=SlackWebhookMock, block_name="test")
            @flow
            def test_flow():
                print("testing")

            @alert_on_failure(block_type=SlackWebhookMock, block_name="test")
            @flow
            def test_async_flow():
                print("testing async")

            assert isinstance(test_flow, prefect.Flow)
            assert isinstance(test_async_flow, prefect.Flow)
