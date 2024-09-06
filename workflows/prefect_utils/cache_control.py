from typing import Any, Dict, Optional

from prefect.context import TaskRunContext
from prefect.utilities.hashing import hash_objects


def context_agnostic_task_input_hash(
    _: "TaskRunContext", arguments: Dict[str, Any]
) -> Optional[str]:
    """
    Similar to Prefect's own task_input_hash, except that we ignore the infrastructure context
    and therefore assert that the persisted result will be available to all consumers of the cached result.
    We also ignore the task key, so that multiple tasks could use the same cache.

    Arguments:
        _: the active `TaskRunContext`
        arguments: a dictionary of arguments to be passed to the underlying task

    Returns:
        a string hash if hashing succeeded, else `None`
    """
    return hash_objects(
        arguments,
    )
