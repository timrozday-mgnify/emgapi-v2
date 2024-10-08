from prefect import task

from analyses.models import Assembly


@task(log_prints=True)
def task_mark_assembly_status(
    assembly: Assembly,
    status: Assembly.AssemblyStates,
    reason: str = None,
    unset_statuses: [Assembly.AssemblyStates] = None,
) -> None:
    """
    Logs and updates the status of a given assembly.
    :param assembly: The assembly object to update.
    :type assembly: Assembly
    :param status: The new status to assign to the assembly.
    :type status: One of Assembly.AssemblyStates
    :param reason: An optional reason for the status change, which will be recorded.
    :type reason: str, optional
    :param unset_statuses: An optional list of statuses to unset, if they are already set (e.g., [Assembly.AssemblyStates.ASSEMBLY_FAILED])
    :type unset_statuses: list of Assembly.AssemblyStates, optional
    :return: None
    :rtype: None
    :raises ValueError: If the status is not one of the predefined AssemblyStates.
    """
    if status not in Assembly.AssemblyStates.__dict__.values():
        raise ValueError(
            f"Invalid status '{status}'. Must be one of the predefined AssemblyStates."
        )

    print(f"Assembly {assembly} status is {status} now.")
    assembly.mark_status(status, reason=reason)
    for unset_status in unset_statuses or []:
        if assembly.status[unset_status]:
            assembly.mark_status(
                unset_status,
                set_status_as=False,
                reason=f"Explicitly unset when setting {status}",
            )
