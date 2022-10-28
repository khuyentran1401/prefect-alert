"""Decorator to send an alert when a task or a flow fails"""
from functools import wraps

import prefect
from prefect.blocks.notifications import AppriseNotificationBlock
from prefect.utilities.asyncutils import is_async_fn

from prefect_alert.utilities import WrappedFlow, _get_alert_message


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

    def decorator(flow):
        if is_async_fn(flow):

            @wraps(flow)
            async def wrapper(*args, **kwargs):
                """A wrapper of an async task/flow"""
                return_state = kwargs.pop("return_state", None)
                state: prefect.State = await flow(*args, return_state=True, **kwargs)

                notification_block: AppriseNotificationBlock = await block_type.load(
                    block_name
                )
                if state.is_failed():
                    message = _get_alert_message(state, flow)
                    await notification_block.notify(message, subject="Flow failed...")
                if return_state:
                    return state
                else:
                    return state.result()

            return WrappedFlow(wrapper)
        else:

            @wraps(flow)
            def wrapper(*args, **kwargs):
                """A wrapper of a sync task/flow"""
                return_state = kwargs.pop("return_state", None)
                state: prefect.State = flow(*args, return_state=True, **kwargs)
                notification_block: AppriseNotificationBlock = block_type.load(
                    block_name
                )
                if state.is_failed():
                    message = _get_alert_message(state, flow)
                    notification_block.notify(message, subject="Flow failed...")
                if return_state:
                    return state
                else:
                    return state.result()

            return WrappedFlow(wrapper)

    return decorator
