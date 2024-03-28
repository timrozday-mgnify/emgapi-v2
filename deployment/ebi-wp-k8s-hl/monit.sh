#!/bin/bash

/nfs/production/rdf/metagenomics/jenkins-slurm/software/monit-5.33.0/bin/monit \
-c /nfs/production/rdf/metagenomics/jenkins-slurm/dev-prefect-agent/deployment/ebi-wp-k8s-hl/monit.rc \
-l /nfs/production/rdf/metagenomics/jenkins-slurm/logs/mi-slurm-worker-monit.log "$@"
