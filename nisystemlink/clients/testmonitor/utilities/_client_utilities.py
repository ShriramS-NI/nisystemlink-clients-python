from typing import List, Optional

from nisystemlink.clients.testmonitor import TestMonitorClient
from nisystemlink.clients.testmonitor.models import (
    QueryStepsRequest,
    Step,
    StepOrderBy,
    StepProjection,
)
from nisystemlink.clients.testmonitor.utilities._constants import HttpConstants


def __query_steps_batched(
    test_monitor_client: TestMonitorClient,
    step_filter: str,
    result_filter: str,
    column_projection: Optional[List[StepProjection]] = None,
    take: int = HttpConstants.DEFAULT_QUERY_STEPS_TAKE,
) -> List[Step]:
    """Queries steps in batches from the test monitor client.

    Args:
        test_monitor_client (TestMonitorClient): The test monitor client instance used to fetch step data.
        step_filter (str): The step linq filter used to query steps.
        result_filter (str): The result linq filter used to query steps. Based on the given result filter,
            only the steps coming under the filtered results would be included in the final response.
        column_projection (Optional[List[StepProjection]]): A list of specific step fields to retrieve.
            Defaults to None, which retrieves all fields.
         take (Optional[int]): The maximum number of steps to query per batch.

    Returns:
        List[Step]: A list of step responses retrieved from the API.
    """
    steps_query = QueryStepsRequest(
        filter=step_filter,
        result_filter=result_filter,
        order_by=StepOrderBy.UPDATED_AT,
        projection=column_projection,
        take=take,
    )

    all_steps: List[Step] = []

    response = test_monitor_client.query_steps(steps_query)
    all_steps.extend(response.steps)
    while response.continuation_token:
        steps_query.continuation_token = response.continuation_token
        response = test_monitor_client.query_steps(steps_query)
        all_steps.extend(response.steps)

    return all_steps
