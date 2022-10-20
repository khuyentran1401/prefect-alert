from prefect import flow, task
from prefect.blocks.notifications import NotificationBlock
from functools import wraps
from typing import Union
import prefect
from prefect.blocks.core import Block
from prefect.utilities.asyncutils import is_async_fn
import asyncio

def alert_on_failure(block_name: str):
    def decorator(flow_or_task: Union[prefect.Flow, prefect.Task]):
        if is_async_fn(flow_or_task):
            print("This is an async func")
            @wraps(flow_or_task)
            async def wrapper(*args, **kwargs):
                return_state = kwargs.pop("return_state", None)
                state: prefect.State = await flow_or_task(*args, return_state=True, **kwargs)
                notification_block: NotificationBlock = await Block.load(block_name)
                if state.is_failed():
                    await notification_block.notify(str(state), subject="Failed run ...")
                if return_state:
                    return state
                else:
                    return state.result()
            return wrapper
        else: 
            print("This is NOT an async func")
            @wraps(flow_or_task)
            def wrapper(*args, **kwargs):
                return_state = kwargs.pop("return_state", None)
                state: prefect.State = flow_or_task(*args, return_state=True, **kwargs)
                notification_block: NotificationBlock = Block.load(block_name)
                if state.is_failed():
                    notification_block.notify(str(state), subject="Failed run ...")
                if return_state:
                    return state
                else:
                    return state.result()
            return wrapper

    return decorator