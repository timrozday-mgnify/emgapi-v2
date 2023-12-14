FROM ubuntu:latest as base
LABEL authors="sandyr"

RUN apt -y update && apt -y upgrade
RUN apt -y install libpq-dev python3-pip python-is-python3

WORKDIR /app
COPY . .
RUN pip install -r requirements.txt

FROM base as django
ENTRYPOINT ["python3", "manage.py"]

FROM base as agent
RUN apt -y update && apt -y upgrade
RUN apt -y install munge gosu netcat-traditional slurm-wlm libslurm-dev
COPY slurm/configs/slurm_single_node.conf /etc/slurm/slurm.conf
RUN chown -R slurm:slurm /etc/slurm/
RUN mkdir -p /run/munge && chown -R munge /run/munge
COPY slurm/entrypoints/submitter-entrypoint.sh /usr/local/bin/submitter-entrypoint.sh
RUN chmod +x /usr/local/bin/submitter-entrypoint.sh
ENV SLURM_INCLUDE_DIR=/usr/include
ENV SLURM_LIB_DIR=/usr/lib/x86_64-linux-gnu
RUN pip install https://github.com/PySlurm/pyslurm/archive/refs/tags/v21.8.1.tar.gz
ENTRYPOINT ["/usr/local/bin/submitter-entrypoint.sh", "python3", "manage.py"]
