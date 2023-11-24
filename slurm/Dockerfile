FROM ubuntu:latest as base
LABEL authors="mgnify team"

RUN apt -y update && apt -y upgrade

RUN apt -y install curl nano wget zip bzip2

RUN apt -y install munge python3-pip python-is-python3

RUN apt -y install sudo gosu netcat-traditional slurm-wlm

#RUN apt -y install hwloc libbpf-dev libdbus-1-dev
#ARG SLURM_URL=https://download.schedmd.com/slurm/slurm-23.02.6.tar.bz2
#ARG SLURM_VER=23.02.6
#
#RUN curl -fsL ${SLURM_URL} | tar xfj - -C /opt/ && \
#    cd /opt/slurm-${SLURM_VER}/ && \
#    ./configure --sysconfdir=/etc/slurm && make && make install
#RUN mkdir -p /sys/fs/cgroup/system.slice/

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
RUN apt install -y slurmdbd
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
ENV SLURM_INCLUDE_DIR=/usr/include
ENV SLURM_LIB_DIR=/usr/lib/x86_64-linux-gnu
RUN DEBIAN_FRONTEND=noninteractive \
    apt install -y default-jre
RUN wget -qO- https://get.nextflow.io | bash
RUN chmod +x nextflow
RUN mv nextflow /usr/bin/
ENTRYPOINT ["/usr/local/bin/worker-entrypoint.sh"]

#FROM worker as submitter
#WORKDIR /app
#COPY .. .
#RUN pip install -r requirements.txt
#ENTRYPOINT ["python", "manage.py"]
