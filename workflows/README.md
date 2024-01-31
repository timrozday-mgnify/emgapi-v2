# Writing Prefect flows for MGnify

## Tasks and flows
[Flows](https://docs.prefect.io/latest/concepts/flows/) and [Tasks](https://docs.prefect.io/latest/concepts/tasks/) are the two building blocks of Prefect workflows.

They are both written as Python functions decorated with `@flow` or `@task`.

Tasks are single steps that need to happen in a workflow. Tasks must be called from flows.

Flows are sequences of tasks, other flows, and arbitrary code.
A flow is the entry point to something being run by Prefect.
A flow can call other flows as subflows.
Flows and tasks can persist results (to storage) which means that if a parent flow needs to be rerun (e.g. because it is paused and resumed), the previously run tasks and flows will not re-execute.

This point is quite important: since tasks and flows look like normal Python functions, you need to remember that a non-decorated function *will* run every time a flow is resumed.

This doesn't usually matter, since in a standard Prefect setup the only time a flow would re-run is if it is retrying on failure.

However, when mixed with long-running HPC jobs (e.g. multi-day nextflow pipelines), our implementation of Prefect will regularly re-run flows.

## Long-running pipelines
Prefect assumes it will control a pipeline throughout execution; i.e. there is a process running on a prefect agent at all times a flow is running.

Consider a typical MGnify flow, though:
* take a study input
* fetch some data from ENA API
* insert something into the EMG database
* submit a cluster job, to run a nextflow pipeline. It may pend for 2 days and run for 4.
* run another pipeline if certain conditions are met
* submit a datamove job
* insert some other things into a database

There are steps here where Prefect needs to cede control, because the prefect agent would be idle, occupying a zombie process for days on end and blocking other processing.

For this reason, we need to make use of the Prefect Scheduler (a database of flow runs and intended start times), so that flows can be "happening" (not strictly running) out-of-process.

We do this by:
1. submitting jobs to the Slurm cluster;
2. checking the state of the Slurm jobs;
3. pausing the prefect flow out of process if the Slurm jobs are not complete;
4. scheduling a "resumer" flow for some duration in the future, which sits in the Prefect database until the scheduler starts it;
5. running the resumer flow, which simply unpauses the main flow.

Steps 2. — 5. are repeated until the Slurm jobs are finished (or perhaps all in terminal states like completed and failed).

There are helpers for this process in `prefect_utils/slurm_flow.py`.

Essentially:

```python
from datetime import timedelta

from prefect import flow
from django.conf import settings
import django
from asgiref.sync import sync_to_async

from workflows.prefect_utils.slurm_flow import run_cluster_jobs, after_cluster_jobs

from some_django_app.models import Study

django.setup()


@flow
async def my_long_flow(study: str):
    samples = sync_to_async(Study.get)(id=study).samples.all()

    await run_cluster_jobs(
        name_pattern="Get read runs for {sample}",
        command_pattern=f"nextflow run {settings.EMG_CONFIG.slurm.pipelines_root_dir}/do_something_slow_with_a_sample.nf --sample={{sample}}",
        jobs_args=[{'sample': sample.id} for sample in samples],
        expected_time=timedelta(seconds=10),
        status_checks_limit=1,
        memory="100M",
    )

    after_cluster_jobs()  # unless a @task follows pause/resumes, prefect won't bother resuming
```

Note the helpers for submitting a single command with several different parameters, using the `pattern`s.

`sync_to_async()` is also needed around the Django functions, since we're in an `async` context.


## Triggering flows from models
A common pattern in production is likely to be triggering a workflow (a Prefect flow) when an object in the EMG
database changes.
For example, when a Study is created, run some prefect flow to fetch it from ENA, and assemble it's reads.
The pattern for this is to use [signals](https://docs.djangoproject.com/en/5.0/topics/signals/).
Signals (e.g., `post_save`) are events/hooks/triggers that call some function e.g. before or after a model is saved.
(Typically `post_save` is preferable, as it limits the amount of possibly failing work done before commiting the thing to the database.)
To keep things clean, flow-invoking-model-hooks should be registered in the `workflows/signals.py` file.

For example:
```python
@receiver(post_save, sender=analyses.models.AssemblyAnalysisRequest)
def on_assembly_analysis_saved(sender, instance: analyses.models.AssemblyAnalysisRequest, created, **kwargs):
    if not created: return
    flowrun = async_to_sync(run_deployment)(
        "Assemble and analyse a study/assembly_analysis_request_deployment", # <-- naming here is NOT arbitrary
        timeout=0,
        parameters={
            "accession": instance.requested_study,
            "request_id": instance.id,
        },
    )
    instance.request_metadata['flow_run_id'] = str(flowrun.id) # <-- potentially useful to save this for admin purposes
    instance.save()
```
