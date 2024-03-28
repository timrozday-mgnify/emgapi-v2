#!/bin/bash

DJANGOPIDFILE=/nfs/production/rdf/metagenomics/jenkins-slurm/processes/dev-mi-slurm-worker-prefect-worker.pid

source /nfs/production/rdf/metagenomics/jenkins-slurm/dev-prefect-agent/deployment/ebi-wp-k8s-hl/secrets-dev-mi-slurm-worker.env

source /hps/software/users/rdf/metagenomics/service-team/software/miniconda_py39/etc/profile.d/conda.sh
conda activate dev-prefect-agent

cd /nfs/production/rdf/metagenomics/jenkins-slurm/dev-prefect-agent/
env HTTP_PROXY="http://127.0.0.1:8080" python manage.py prefectcli worker start --pool "slurm" >> /nfs/production/rdf/metagenomics/jenkins-slurm/logs/dev-mi-slurm-worker-prefect-worker.log 2>&1 &

echo $! > "${DJANGOPIDFILE}"
