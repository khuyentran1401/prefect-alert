"""Decorator to send an alert when a task or a flow fails"""
from functools import wraps
from typing import Union

import prefect
from prefect.blocks.notifications import AppriseNotificationBlock
from prefect.utilities.asyncutils import is_async_fn


def alert_on_failure(block_type: AppriseNotificationBlock, block_name: str):
    """Decorator to send an alert when a task or a flow fails

    This decorator must be placed before your `@flow` or `@task` decorator.

    Args:
        block_type: Type of your notification block (.i.e, 'SlackWebhook')
        block_name: Name of your notification block (.i.e, 'test')

    Examples:
        Send a notification when a flow fails
        ```python
        from prefect import flow, task
        from prefect.blocks.notifications import SlackWebhook
        from prefect_alert import alert_on_failure

        @task
            def may_fail():
                raise ValueError()

        @alert_on_failure(block_type=SlackWebhook, block_name="test")
        @flow
        def failed_flow():
            res = may_fail()
            return res

        if __name__=="__main__":
            failed_flow()
        ```

    """

    def decorator(flow_or_task: Union[prefect.Flow, prefect.Task]):
        if is_async_fn(flow_or_task):

            @wraps(flow_or_task)
            async def wrapper(*args, **kwargs):
                """A wrapper of an async task/flow"""
                return_state = kwargs.pop("return_state", None)
                state: prefect.State = await flow_or_task(
                    *args, return_state=True, **kwargs
                )
                notification_block: AppriseNotificationBlock = await block_type.load(
                    block_name
                )
                if state.is_failed():
                    await notification_block.notify(
                        str(state), subject="Failed run ..."
                    )
                if return_state:
                    return state
                else:
                    return state.result()

            return wrapper
        else:

            @wraps(flow_or_task)
            def wrapper(*args, **kwargs):
                """A wrapper of a sync task/flow"""
                return_state = kwargs.pop("return_state", None)
                state: prefect.State = flow_or_task(*args, return_state=True, **kwargs)
                notification_block: AppriseNotificationBlock = block_type.load(
                    block_name
                )
                if state.is_failed():
                    notification_block.notify(str(state), subject="Failed run ...")
                if return_state:
                    return state
                else:
                    return state.result()

            return wrapper

    return decorator
