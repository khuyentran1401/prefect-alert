"""Contains useful functions for the decorator"""
from functools import update_wrapper
from uuid import UUID

import prefect
from prefect.settings import PREFECT_API_URL


def _get_flow_run_page_url(flow_run_id: UUID) -> str:
    """
    Returns a link to the flow run page.
    Args:
        flow_run_id: the flow run id.
    """
    api_url = PREFECT_API_URL.value() or "http://ephemeral-orion/api"
    ui_url = (
        api_url.replace("api", "app")
        .replace("app/accounts", "account")
        .replace("workspaces", "workspace")
    )
    return f"{ui_url}/flow-runs/flow-run/{flow_run_id}"


def _get_alert_message(state: prefect.State, flow: prefect.Flow):
    """Get alert message for a specific flow run"""
    flow_name = flow.name
    flow_run_id = state.state_details.flow_run_id
    url = _get_flow_run_page_url(flow_run_id)
    return f"The flow {flow_name} fails. \n Summary: {str(state)}). \n Details: {url}"


class WrappedFlow(prefect.Flow):
    """Updates the wrapper function to a Prefect flow"""

    def __init__(self, wrapper):
        if type(wrapper.__wrapped__) != prefect.Flow:
            raise RuntimeError("This class can only wrap Prefect flows.")

        self.wrapper = wrapper
        update_wrapper(self, wrapper.__wrapped__)

    def __call__(self, *args, **kwargs):
        return self.wrapper(*args, **kwargs)
