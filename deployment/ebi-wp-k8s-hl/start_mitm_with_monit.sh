#!/bin/bash

MITMPIDFILE=/nfs/production/rdf/metagenomics/jenkins-slurm/processes/dev-mi-slurm-worker-mitmproxy.pid

source /nfs/production/rdf/metagenomics/jenkins-slurm/dev-prefect-agent/deployment/ebi-wp-k8s-hl/secrets-dev-mi-slurm-worker.env

source /nfs/production/rdf/metagenomics/jenkins-slurm/dev-prefect-agent/venv/bin/activate

cd /nfs/production/rdf/metagenomics/jenkins-slurm/dev-prefect-agent/
mitmdump --mode upstream:http://hh-wwwcache.ebi.ac.uk:3128 -s deployment/ebi-wp-k8s-hl/mitm_auth_for_workers.py >> /nfs/production/rdf/metagenomics/jenkins-slurm/logs/dev-mi-slurm-mitmproxy.log 2>&1 &
echo $! > "${MITMPIDFILE}"
