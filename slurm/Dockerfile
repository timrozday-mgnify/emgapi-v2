FROM ubuntu:latest as base
LABEL authors="mgnify team"

# As root
RUN apt update && apt -y upgrade

RUN DEBIAN_FRONTEND=noninteractive \
    apt -y install build-essential \
                   curl \
                   dos2unix \
                   git \
                   htop \
                   lshw \
                   nano \
                   wget \
                   zip

RUN DEBIAN_FRONTEND=noninteractive \
    apt -y install munge \
                   net-tools \
                   openssl \
                   python3-pip \
                   slurm-wlm \
                   slurm-wlm-doc \
                   slurm-wlm-basic-plugins \
                   slurm-wlm-basic-plugins-dev \
                   slurmdbd

RUN DEBIAN_FRONTEND=noninteractive \
    apt -y install sudo gosu netcat

COPY slurm.conf /etc/slurm/slurm.conf
COPY slurmdbd.conf /etc/slurm/slurmdbd.conf
RUN chmod 600 /etc/slurm/slurmdbd.conf
RUN chown -R slurm:slurm /etc/slurm/

RUN mkdir -p /run/munge && chown -R munge /run/munge
RUN mkdir -p /var/run/slurm && chown -R slurm:slurm /var/run/slurm
RUN mkdir -p /var/log/slurm && chown -R slurm:slurm /var/log/slurm
RUN mkdir -p /var/spool/slurmctld && chown -R slurm:slurm /var/spool/slurmctld
RUN mkdir -p /var/spool/slurmd && chown -R slurm:slurm /var/spool/slurmd

FROM base as dbd
COPY dbd-entrypoint.sh /usr/local/bin/dbd-entrypoint.sh
RUN chmod +x /usr/local/bin/dbd-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/dbd-entrypoint.sh"]

FROM base as ctl
COPY ctl-entrypoint.sh /usr/local/bin/ctl-entrypoint.sh
RUN chmod +x /usr/local/bin/ctl-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/ctl-entrypoint.sh"]

FROM base as worker
COPY worker-entrypoint.sh /usr/local/bin/worker-entrypoint.sh
RUN chmod +x /usr/local/bin/worker-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/worker-entrypoint.sh"]
