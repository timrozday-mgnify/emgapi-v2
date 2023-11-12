FROM python:3.11.5-bullseye as base
LABEL authors="sandyr"

WORKDIR /app
COPY . .
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "manage.py"]

# --------------- #

FROM base as agent
RUN apt update -y && apt install -y default-jre
WORKDIR /tmp
RUN wget -qO- https://get.nextflow.io | bash
RUN chmod +x nextflow
RUN mv nextflow /usr/bin/

WORKDIR /app
