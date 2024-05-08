import json
import logging
from typing import Optional, Union
from uuid import UUID

from django.utils.text import slugify
from prefect import flow, get_client, resume_flow_run, task, suspend_flow_run
from prefect.artifacts import create_table_artifact
from prefect.client.schemas import FlowRun
from prefect.deployments import run_deployment
from prefect.runtime import flow_run
from prefect.task_runners import SequentialTaskRunner


@task(persist_result=True)
async def _launch_out_of_process_subflow(
    deployment_name: str,
    parent_flow_run_id: Union[UUID, str],
    parameters: Optional[dict] = None,
    make_globally_idempotent: bool = False,
) -> Union[UUID, str]:
    if make_globally_idempotent:
        idempotency_key = (
            f"{deployment_name}-shared-subflow-{hash(json.dumps(str(parameters)))}"
        )
    else:
        idempotency_key = f"{deployment_name}-subflow-of-{parent_flow_run_id}-{hash(json.dumps(str(parameters)))}"

    subflow_run: FlowRun = await run_deployment(
        name=deployment_name,
        as_subflow=False,
        idempotency_key=slugify(idempotency_key),
        parameters=parameters,
    )
    return subflow_run.id


@task(persist_result=True)
async def _resume_parent_flow_if_paused(parent_flow_run_id: Union[UUID, str]):
    async with get_client() as client:
        parent_flow_run = await client.read_flow_run(parent_flow_run_id)
        if parent_flow_run.state.is_paused():
            logging.info(f"Resuming parent run {parent_flow_run_id}...")
            await resume_flow_run(parent_flow_run_id)


@flow(task_runner=SequentialTaskRunner)
async def out_of_process_subflow_monitor(
    subflow_run_id: Union[UUID, str],
    parent_flow_run_id: Union[UUID, str],
):
    """
    Monitor an out-of-process subflow.
    Flowruns of this monitor flow are like intermediates between a flowrun and a subflowrun,
    used to monitor the OOP subflow, and coordinate a return to the parent flow.

    This orchestrator/monitor flow should NOT be called as a subflow,
    as it needs to be independently suspended+resumed.

    :param subflow_run_id: The run ID of the subflow that should be monitored.
    :param parent_flow_run_id: The ID of prefect flow-run that will be resumed if/when the subflow finishes.
        Typically, this is the LOGICAL parent flowrun (not formal parent flowrun) that was awaiting this job.
    :return:
    """
    if flow_run.parent_flow_run_id is not None:
        raise ValueError(
            f"out_of_process_subflow_monitor must not be called as a subflow, since subflows cannot be paused."
        )

    #########################
    # PERIODIC OBSERVATIONS #
    #########################
    async with get_client() as client:
        subflow_run = await client.read_flow_run(subflow_run_id)

        if not subflow_run.state.is_final():
            logging.info(
                f"Subflow {subflow_run_id} is in state {subflow_run.state}. "
                f"Doing nothing to parent run {parent_flow_run_id}, "
                f"but pausing this orchestrator flow run {flow_run.id} for another iteration."
                f"This is iteration {flow_run.run_count}"
            )
            await suspend_flow_run()

        else:
            logging.info(
                f"Subflow {subflow_run_id} is in final state {subflow_run.state}."
            )

    ###############
    # FINAL STATE #
    ###############
    # When subflow done, resume parent.
    # Unlikely to happen on first invocation, unless it immediately failed.
    # Because this is an orchestratable task, the previous `suspend_flow_run()` will kick in before this
    # if the subflow is not yet done.
    await _resume_parent_flow_if_paused(parent_flow_run_id)


@flow(persist_result=True, log_prints=True, task_runner=SequentialTaskRunner)
async def await_out_of_process_subflow(
    subflow_deployment_name: str,
    parameters: Optional[dict] = None,
    make_globally_idempotent: bool = False,
) -> FlowRun:
    """
    Convenience flow to run a prefect flow as an out-of-process subflow.

    Out-of-process means that the "subflow" is actually a top level flowrun of its own.
    This is so that it can be paused/resumed on its own, all the while the "parent" flowrun may be continuously paused.

    Await means that the calling (i.e. parent of this) flowrun will be suspended
    until the subflow finishes in some final state.

    The subflow must be DEPLOYED (i.e. have a prefect deployment) for this to be possible.

    An intermediary flowrun, of the deployment
    `out-of-process-subflow-monitor/out-of-process-subflow-monitor-deployment`,
    will be created to manage the connection between the calling awaiting flow and the created awaited "sub"flow.

    :param subflow_deployment_name: The deployment name of the subflow to orchestrate,
        in the form "my-flow/my-deployment".
    :param parameters: Dict of parameters to pass to the subflow run.
    :param make_globally_idempotent: If true, the subflow idempotency key will NOT include the `parent_flow_run_id`:
        i.e. the same instance of the subflow COULD be used by another caller/parent-flow of this.
        This may be useful in cases where two top levels need to await the same subflow,
        e.g. to operate on the same input dataset.
    :return: The orchestrated flowrun of the out-of-process subflow.
        The intermediate flowrun is not returned, but can be found in the created artifact.
    """

    if flow_run.parent_flow_run_id is None:
        raise ValueError(f"await_out_of_process_subflow must be called as a subflow.")

    # Launch the subflow via a deployment
    subflow_run_id = await _launch_out_of_process_subflow(
        deployment_name=subflow_deployment_name,
        parent_flow_run_id=flow_run.parent_flow_run_id,
        parameters=parameters,
        make_globally_idempotent=make_globally_idempotent,
    )
    print(
        f"Orchestrated subflow run {subflow_run_id} as an 'out-of-process' subflow of {flow_run.parent_flow_run_id}"
    )

    # Launch the monitor as a deployment
    monitor_run_id = await run_deployment(
        name="out-of-process-subflow-monitor/out_of_process_subflow_monitor_deployment",
        as_subflow=False,
        idempotency_key=flow_run.id,
        parameters={
            "subflow_run_id": subflow_run_id,
            "parent_flow_run_id": flow_run.parent_flow_run_id,
        },
    )
    print(f"Will be monitored by monitor flow run {monitor_run_id}")

    # Pause the parent flow of this
    await suspend_flow_run(flow_run.parent_flow_run_id)
    print(f"Suspended parent flow run {flow_run.parent_flow_run_id}")

    # Return the subflow run, so we are API compatible with having run prefect-native
    # run_deployment(subflow, as_subflow=False)
    async with get_client() as client:
        subflow_run = await client.read_flow_run(subflow_run_id)

    await create_table_artifact(
        key="out-of-process-subflow",
        table=[
            {
                "subflow_deployment_name": subflow_deployment_name,
                "subflow_run_id": subflow_run_id,
                "monitor_run_id": monitor_run_id,
                "parent_flow_run_id": flow_run.parent_flow_run_id,
            }
        ],
        description="# Out of process subflow orchestration details",
    )

    return subflow_run
