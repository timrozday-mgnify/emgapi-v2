from typing import List
from prefect import task

from emgapiv2.settings import EMG_CONFIG
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash

import httpx
import ena.models
import analyses.models

RESULT_FORMAT = "json"


@task(
    retries=2,
    cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Get study from ENA: {accession}",
    log_prints=True,
)
def get_study_from_ena(accession: str, limit: int = 10) -> ena.models.Study:
    fields = ",".join(["study_title", "secondary_study_accession"])

    if ena.models.Study.objects.filter(accession=accession).exists():
        return ena.models.Study.objects.get(accession=accession)
    print(f"Will fetch from ENA Portal API Study {accession}")
    portal = httpx.get(
        f"{EMG_CONFIG.ena.portal_search_api}?result=study&query=study_accession%3D{accession}%20OR%20secondary_study_accession%3D{accession}&limit={limit}&format={RESULT_FORMAT}&fields={fields}"
    )
    if portal.status_code == httpx.codes.OK:
        s = portal.json()[0]
        primary_accession: str = s["study_accession"]
        secondary_accession: str = s["secondary_study_accession"]
        study, created = ena.models.Study.objects.get_or_create(
            accession=primary_accession,
            defaults={
                "title": portal.json()[0]["study_title"],
                "additional_accessions": [secondary_accession],
                # TODO: more metadata
            },
        )
        return study
    else:
        raise Exception(f"Bad status! {portal.status_code} {portal}")


@task(
    retries=10,
    retry_delay_seconds=60,
    cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Get study readruns from ENA: {accession}",
    log_prints=True,
)
def get_study_readruns_from_ena(
    accession: str, limit: int = 20, filter_library_strategy: str = None
) -> List[str]:
    fields = ",".join(
        [
            "sample_accession",
            "sample_title",
            "secondary_sample_accession",
            "fastq_md5",
            "fastq_ftp",
            "library_layout",
            "library_strategy",
        ]
    )

    print(f"Will fetch study {accession} read-runs from ENA portal API")
    mgys_study = analyses.models.Study.objects.get(ena_study__accession=accession)
    query = (
        f'query="study_accession={accession} OR secondary_study_accession={accession}'
    )
    if filter_library_strategy:
        query += f" AND library_strategy=%22{filter_library_strategy}%22"
    query += '"'
    portal = httpx.get(
        f"{ENA_SEARCH_API}?result=read_run&dataPortal=metagenome&format={RESULT_FORMAT}&fields={fields}&{query}&limit={limit}"
    )

    if portal.status_code == httpx.codes.OK:
        for read_run in portal.json():
            ena_sample, _ = ena.models.Sample.objects.get_or_create(
                accession=read_run["sample_accession"],
                defaults={
                    "metadata": {"sample_title": read_run["sample_title"]},
                    "study": mgys_study.ena_study,
                },
            )

            mgnify_sample, _ = analyses.models.Sample.objects.update_or_create(
                ena_sample=ena_sample,
                defaults={
                    "ena_accessions": [
                        read_run["sample_accession"],
                        read_run["secondary_sample_accession"],
                    ],
                    "ena_study": mgys_study.ena_study,
                },
            )

            analyses.models.Run.objects.update_or_create(
                ena_accessions=[read_run["run_accession"]],
                study=mgys_study,
                ena_study=mgys_study.ena_study,
                sample=mgnify_sample,
                defaults={
                    "metadata": {
                        "library_strategy": read_run["library_strategy"],
                        "library_layout": read_run["library_layout"],
                        "fastq_ftps": list(read_run["fastq_ftp"].split(";")),
                    }
                },
            )

    mgys_study.refresh_from_db()
    return [run.ena_accessions[0] for run in mgys_study.runs.all()]
