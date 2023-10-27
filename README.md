# Prototype EMG backlog/API, using Prefect

There are two real Django apps here: 
* `ena`, for models that mirror objects in ENA: studies, samples, etc.
* `analyses`, for models associated with MGnify analysis production work (MGYS, MGYA etc).
* ... other models like genomes, proteins, could live in separate apps.

There is one fake Django app `workflows`, which is used to tie Prefect (the workflow scheduler) into Django.
This is bidi: it creates a `manage.py prefectcli` command to run Prefect, and it allows Prefect tasks to use instantiated Django.

The API is implemented with `ninja` (`emgapiv2/api.py`).

## Running it
There are three parts – an API server, a Prefect server, and a Prefect agent.
In a real world these would probably live on three seperate VMs (or more likely; an API server and Prefect server on K8s, a Prefect agent on K8s and another Prefect agent on Codon).
We would probably have a dev env using k3s to test all of this, but I didn't get it working yet.
The Prefect server doesn't need all the same requirements as the agent and API – it could just be the upstream docker image.

### Make an env
```shell
conda create --name prefect-prototype python==3.11.5
conda activate prefect-prototype
pip install -r requirements.txt
```

### Make Django db
```shell
python manage.py migrate
```

### Run API
```shell
python manage.py runserver
```

### Register the prefect flows (new shell)
```shell
python manage.py prefectcli deployment build workflows/mgnify_run_pipeline_flow.py:mgnify_run_pipeline_flow --name mgnify_run_pipeline_deployment --output workflows/mgnify_run_pipeline_flow_deployment.yaml
python manage.py prefectcli deployment build workflows/ena_fetch_study_flow.py:ena_fetch_study_flow --name ena_fetch_study_deployment --output workflows/ena_fetch_study_flow_deployment.yaml 
python manage.py prefectcli deployment apply workflows/mgnify_run_pipeline_flow_deployment.yaml
python manage.py prefectcli deployment apply workflows/ena_fetch_study_flow_deployment.yaml 
```

### Run the Prefect server
```shell
conda activate prefect-prototype
python manage.py prefectcli server start
```

### Run the Prefect agent (another new shell)
```shell
conda activate prefect-prototype
python manage.py prefectcli agent start -q default
```


### Run a flow
Either: open the [Prefect dashboard](http://localhost:4200), or use a POST request on the [MGnify API](http://localhost:8000/api/v2/), or use the prefect CLI via docker compose.
E.g. kick off the "ENA fetch studies and samples" flow with a PRJxxxxx accession.