[![Testing](https://github.com/EBI-Metagenomics/emgapi-v2/actions/workflows/test.yml/badge.svg)](https://github.com/EBI-Metagenomics/emgapi-v2/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/EBI-Metagenomics/emgapi-v2/branch/main/graph/badge.svg?token=27IVW899W8)](https://codecov.io/gh/EBI-Metagenomics/emgapi-v2)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# Prototype EMG backlog/API, using Prefect

There are two real Django apps here:
* `ena`, for models that mirror objects in ENA: studies, samples, etc.
* `analyses`, for models associated with MGnify analysis production work (MGYS, MGYA etc).
* ... other models like genomes, proteins, could live in separate apps.

There is one fake Django app `workflows`, which is used to tie Prefect (the workflow scheduler) into Django.
This is bidi: it creates a `manage.py prefectcli` command to run Prefect, and it allows Prefect tasks to use instantiated Django.

The API is implemented with `ninja` (`emgapiv2/api.py`), and uses Open API spec with Swagger.

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
E.g. following [the docker docs](https://docs.docker.com/compose/install/) or using Podman or Colima, as you prefer. In theory all should work.

### The taskfile
The project has a taskfile to simplify some common activities. So, [install Task](https://taskfile.dev/installation/).

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
> #### Details
> Be aware this runs 7 containers using ~2GB of RAM. Configure your Podman Machine / Docker Desktop / Colima setup accordingly.
>
> You'll see logs from all the containers.
>
> Depending on your containerisation setup, you may need to tweak the `CPUs=4` line of `slurm/configs/slurm_single_node.conf:45`, e.g. setting it to 1.
> This is related to the number of CPUs on your host machine or on the container VM you're using: e.g. what you set in Docker Desktop.

You can then go to http://127.0.0.1:4200 to see the Prefect dashboard (workflows to be run).
You can also go to http://localhost:8000/api/v2/docs to see the Django app.

### Run a basic Prefect flow
Prefect flows are just Python. There is a hello-world like example in `workflows/flows/simple_example.py`.
It can be run using Python, e.g. inside the `app` container:
```shell
 docker-compose exec app python workflows/flows/simple_example.py
```
You'll see that the flow and task decorators break the workflow up into individually executable bits of work.
You can use this kind of approach to debug things.
Meaningful flows, however, are run on separate infrastructure – and that is what the slurm and prefect agent dev environments are for.


### Register a Prefect flow (new shell)
```shell
 FLOW=realistic_example task deploy-flow
```
This "builds" a prefect flow (from the `workflows/flows/` directory, in a file of name `realstic_example` with an `@flow`-decorated method also called `realistic_example`).
(Have a look at the Taskfile if you want to remove this same-named constraint.)
It also "applies" the "flow deployment", which means the Prefect server knows how to execute it.
It will register it as requiring an "hpc" worker agent to run it.
The Prefect agent in the docker compose setup is labelled as being this "hpc" agent, so will pick it up.
This "hpc" agent simulates a worker node on an HPC cluster, e.g. it can submit `nextflow` pipeline executions.
In a real world these nextflow pipelines might also have a config to use an HPC scheduler themselves, like launching child Slurm jobs.

### Run a flow
Either: open the [Prefect dashboard](http://localhost:4200), or use a POST request on the [MGnify API](http://localhost:8000/api/v2/), or use the prefect CLI via docker compose.

E.g., use the Prefect dashboard to do a "quick run" of the [Realistic Example flow you just deployed](http://localhost:4200/deployments?deployments.nameLike=realistic&page=1) with accession `PRJNA521078`.
This example will:
- make an ENA Study in the database
- suspend itself and wait to be "resumed" in the Prefect dashboard, because it needs to know a "sample limit" from the admin user
- get a list of samples from the ENA API, in an @task
- run a nextflow pipeline for each sample, on slurm, that downloads the read runs

You could also run this newly deployed flow from the command line, using the Prefect CLI. e.g.:
```shell
task prefect -- deployment run "Download a study read-runs/realistic_example_deployment" --param accession=PRJNA521078
```

(Note that you can't run this one in the same way as `simple_example.py`, because `realistic_example.py` does not have a `__main__`).

### Interacting with Slurm
See [the slurm/README.md](slurm/README.md) for details. In short: `task slurm` and you're on a slurm node.


## Writing flows
See [the workflows/README.md](workflows/README.md) for details. In short: add Python/Prefect code to a file in `workflows/flows/` and then `FLOW=my_flow task deploy-flow`.

## Testing
The project uses the [pytest](https://docs.pytest.org) framework.
[Prefect has some helpers](https://docs.prefect.io/latest/guides/testing/) for testing.
We also use [Pytest-django](https://pytest-django.readthedocs.io/en/latest/) to help with Django testing.

Testing libraries are in `requirements-dev.txt`. These are installed in the docker compose `app` container. So:

```shell
task test
# ...will run everything. Or for a subset, use pytest arguments after -- e.g.:
task test -- -k study
```

## Deployment
The `deployment/` folder has deployment configs for different environments.
Each should have its own `Taskfile`, included in the main `Taskfile`.
E.g. see [the EBI WP K8s HL deployment README](deployment/ebi-wp-k8s-hl/README.md).
Run e.g. `task ebi-wp-k8s-hl:update-api` to build/push/restart the EMG API service in that deployment (**requires some secrets setup**).


## TODO
* DB Schema parity with EMG DB (v1) and EMG Backlog
* Job cleanup flows
* Legacy data importers
