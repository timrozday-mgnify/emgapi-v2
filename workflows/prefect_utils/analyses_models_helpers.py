from prefect import task

from analyses.models import Assembly


@task(log_prints=True)
def task_mark_assembly_status(
    assembly: Assembly, status: str, reason: str = None
) -> None:
    """
    Logs and updates the status of a given assembly.
    :param assembly: The assembly object to update.
    :type assembly: Assembly
    :param status: The new status to assign to the assembly.
    :type status: One of Assembly.AssemblyStates
    :param reason: An optional reason for the status change, which will be recorded.
    :type reason: str, optional
    :return: None
    :rtype: None
    :raises ValueError: If the status is not one of the predefined AssemblyStates.
    """
    if status not in Assembly.AssemblyStates.__dict__.values():
        raise ValueError(
            f"Invalid status '{status}'. Must be one of the predefined AssemblyStates."
        )

    print(f"Assembly {assembly} (run {assembly.run}) status is {status} now.")
    assembly.mark_status(status, reason=reason)
