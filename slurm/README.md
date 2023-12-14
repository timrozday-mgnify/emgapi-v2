# Docker-composed Slurm cluster for local development

This directory is a configuration of a minimal but functioning Slurm cluster, running on Docker Compose,
intended for development and testing of Slurm-integrated code, e.g. job submission/running/polling via [PySlurm](https://github.com/PySlurm/pyslurm).

It also includes helper type config for running [Prefect](https://prefect.io) flows on the Slurm cluster.

## Running it
From the parent directory, `docker compose --profile slurm up` will build and start the Slurm cluster.
This `include`s the `docker-compose.yaml` file in this `slurm` dir.

## What you get
It starts:
* `slurm_db`: a Maria DB host for the `slurm` database. Port `3306` is mapped to your host machine, so you can inspect the Slurm Job DB at `mariadb://slurm:slurm@localhost:3306/slurm` (see e.g. `donco_job_table`).
* `slurm_node`: a container running the slurm database daemon, controller, and a worker node (see below).
* The alternative setup (profile `slurm_full`, not started by default) separates these into separate containers for a more realistic setup:
  * `slurm_db_daemon`: a container running the [Slurm database daemon](https://slurm.schedmd.com/slurmdbd.html). This is the interface between Slurm and the above DB.
  * `slurm_controller`: a container running the [Slurm controller / central manager daemon](https://slurm.schedmd.com/slurmctld.html). This is the node workers poll for jobs.
  * `slurm_worker`: a container running the [Slurm compute node daemon](https://slurm.schedmd.com/slurmd.html). This container executes jobs in the queue.
  * Note the difference in `congigs/` and `entrypoints/` for these single node vs. full setups.

Other than the Maria DB container, the others share a common Ubuntu-based image from `Dockerfile`.
The `*-entrypoint.sh` scripts start the respective daemons in the respective containers.

`submitter-entrypoint.sh` is not used by any of these containers, but can be used to make a "submission node" (AKA "login node").
That means a node that has a Slurm installation and still runs [munged](https://linux.die.net/man/8/munged) so that the submission node can submit jobs, but it doesn't do work.
This is used by the parent docker-compose setup, to submit Slurm jobs from Prefect flows.

`slurm*.conf` are configs made using [the configurator](https://slurm.schedmd.com/configurator.html) for this docker compose setup only.

## Using it
### Interactively on the nodes
The Slurm cluster is called `donco` (not `codon` :-] ).
You can dispatch Slurm commands on one of the nodes, e.g. the `slurm_node` (single node setup) or `slurm_worker` or `_controller` in the full setup.

```shell
user@host:/# docker exec -it slurm_node bash
root@slurm_node:/# sinfo
# PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST
# debug*       up   infinite      1  alloc slurm_node
root@slurm_node:/# sbatch --wait -t 00:00:30 --mem 10M --wrap="ls" -o listing.txt
# Submitted batch job 54
root@slurm_node:/# sacct -j 54
# JobID           JobName  Partition    Account  AllocCPUS      State ExitCode
# ------------ ---------- ---------- ---------- ---------- ---------- --------
# 54                 wrap      debug       root          1  COMPLETED      0:0
# 54.batch          batch                  root          1  COMPLETED      0:0
```

### Via Taskfile convenience wrapper
From the parent dir, where there is a Taskfile:
`task slurm` will open a shell on the slurm controller node, where you can run things like `sinfo`, `squeue`, `srun`, `sbatch` etc.

`task sbatch -- --wait -t 00:00:30 --mem 10M --wrap="ls" -o listing.txt` will dispatch a job directly.

Use e.g. `docker logs slurm_worker -f` to see the job execute.
