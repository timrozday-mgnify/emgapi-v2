# Prototype EMG backlog/API, using Prefect

There are two real Django apps here:
* `ena`, for models that mirror objects in ENA: studies, samples, etc.
* `analyses`, for models associated with MGnify analysis production work (MGYS, MGYA etc).
* ... other models like genomes, proteins, could live in separate apps.

There is one fake Django app `workflows`, which is used to tie Prefect (the workflow scheduler) into Django.
This is bidi: it creates a `manage.py prefectcli` command to run Prefect, and it allows Prefect tasks to use instantiated Django.

The API is implemented with `ninja` (`emgapiv2/api.py`).

## Dev setup
Clone the repo.
`pip install -r requirements-dev.txt` (or just have `pre-commit` installed somehow).

`pre-commit install`.

## Running it
There are three main parts – an API server, a Prefect server, and a Prefect agent.
(There are also database and object stores to run – these can be sqlite/local-fs, but this aims to be a more production-like setup.)
In a real world these would probably live on separate VMs: on HPC, on hosted DBs, and on K8s.
For local development, these are all run in a docker-compose environment.

There is also a docker-compose setup of [Slurm](https://slurm.schedmd.com), so that automation of HPC scheduling can be developed.
This creates a tiny slurm cluster called `donco` (not `codon`).
This is in the `slurm` directory: see [slurm/README.md](slurm/README.md) for more.

### Set up docker-compose
E.g. following [the docker docs](https://docs.docker.com/compose/install/) or using Podman, as you prefer.

### The taskfile
The project has a taskfile to simplify some common activities.

### Make the Django DB
```shell
task manage -- migrate
```
This will have created a Django-managed DB on a dockerized Postgres host.

### Run everything (the databases, the Django app, the Prefect workflow server, a Prefect work egent, and a small Slurm cluster with associated controllers+dbs.)
```shell
task deploy-utils
task run
```
(Be aware this runs 9 containers using ~2GB of RAM. Configure your Podman Machine / Docker Desktop setup accordingly.)
You'll see logs from all the containers.
You can then go to http://127.0.0.1:4200 to see the Prefect dashboard (workflows to be run).
You can also go to http://127.0.0.1:8000 to see the Django app.

### Register the Prefect flows (new shell)
```shell
FLOW=ena_fetch_study_flow task deploy-flow
```
This "builds" a prefect flow (from the `workflows/flows/` directory, in a file of name `ena_fetch_study_flow` with an `@flow`-decorated method also called `ena_fetch_study_flow`).
It also "applies" the "flow deployment", which means the Prefect server knows how to execute it.
It will register it as requiring an "hpc" worker agent to run it.
The Prefect agent in the docker compose setup is labelled as being this "hpc" agent, so will pick it up.
This "hpc" agent simulates a worker node on an HPC cluster, e.g. it can submit `nextflow` pipeline executions.
In a real world these nextflow pipelines might have a config to use an HPC scheduler like Slurm.

### Run a flow
Either: open the [Prefect dashboard](http://localhost:4200), or use a POST request on the [MGnify API](http://localhost:8000/api/v2/), or use the prefect CLI via docker compose.
E.g. kick off the "ENA fetch studies and samples" flow with a PRJxxxxx accession.
This example flow will call the ENA API to list samples for the project, create entities in the DB for them, and then launch a nextflow pipeline to fetch read-run FASTQ files for each sample.

### Interacting with Slurm
See [the slurm/README.md](slurm/README.md) for details. In short: `task slurm`.


## Writing flows
See [the workflows/README.md](workflows/README.md) for details. In short: add Python/Prefect code to a file in `workflows/flows/` and then `FLOW=my_flow task deploy-flow`.

## TODO
* DB Schema for merged EMG + Backlog DB
* JSON:API in Django Ninja
* Slurm-job cancellation on flow failure (using Prefect state change hooks)
* Job cleanup flows
* Flow test
