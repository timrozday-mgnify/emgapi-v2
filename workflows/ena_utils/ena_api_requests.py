from typing import List

import httpx
from prefect import task, get_run_logger
import analyses.models
import ena.models
from emgapiv2.settings import EMG_CONFIG
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash

ALLOWED_LIBRARY_SOURCE: list = ["METAGENOMIC", "METATRANSCRIPTOMIC"]
SINGLE_END_LIBRARY_LAYOUT: str = "SINGLE"
PAIRED_END_LIBRARY_LAYOUT: str = "PAIRED"
METAGENOME_SCIENTIFIC_NAME: str = "metagenome"


def create_ena_api_request(result_type, query, limit, fields, result_format="json"):
    return f"{EMG_CONFIG.ena.portal_search_api}?" \
           f"result={result_type}&" \
           f"query={query}&" \
           f"limit={limit}&" \
           f"format={result_format}&" \
           f"fields={fields}"


@task(
    retries=2,
    cache_key_fn=context_agnostic_task_input_hash,
    task_run_name="Get study from ENA: {accession}",
    log_prints=True,
)
async def get_study_from_ena(accession: str, limit: int = 10) -> ena.models.Study:
    logger = get_run_logger()

    fields = ",".join(EMG_CONFIG.ena.study_metadata_fields)
    result_type = "study"
    query = f"study_accession%3D{accession}%20OR%20secondary_study_accession%3D{accession}"

    logger.info(f"Will fetch from ENA Portal API Study {accession}")
    portal = httpx.get(
        create_ena_api_request(
            result_type=result_type,
            query=query,
            limit=limit,
            fields=fields
        )
    )
    if portal.status_code == httpx.codes.OK:
        response_json = portal.json()
        # Check if the response is empty
        if not response_json:
            raise Exception(f"No study found for accession {accession}")
        s = response_json[0]

        # Check secondary accession
        secondary_accession: str = s["secondary_study_accession"]
        additional_accessions: list = []
        if not secondary_accession:
            logger.warning(f"Study {accession} secondary_accession is not available")
        else:
            if len(secondary_accession.split(';')) > 1:
                logger.warning(f"Study {accession} has more than one secondary_accession")
                additional_accessions = secondary_accession.split(';')
            else:
                additional_accessions = [secondary_accession]

        # Check primary accession
        if not s["study_accession"]:
            logger.warning(f"Study {accession} primary_accession is not available. "
                           f"Use first secondary accession as primary_accession")
            if additional_accessions:
                primary_accession = additional_accessions[0]
            else:
                raise Exception(f"Neither primary nor secondary accessions found for study {accession}")
        else:
            primary_accession: str = s["study_accession"]

        study, created = await ena.models.Study.objects.aget_or_create(
            accession=primary_accession,
            defaults={
                "title": portal.json()[0]["study_title"],
                "additional_accessions": additional_accessions,
                # TODO: more metadata
            },
        )
        return study
    else:
        raise Exception(f"Bad status! {portal.status_code} {portal}")


def check_reads_fastq(fastq: list, run_accession: str, library_layout: str):
    logger = get_run_logger()
    sorted_fastq = sorted(fastq)  # to keep order [_1, _2, _3(?)]
    if not len(sorted_fastq):
        logger.warning(f'No fastq files for run {run_accession}')
        return False
    # potential single end
    elif len(sorted_fastq) == 1:
        if library_layout == PAIRED_END_LIBRARY_LAYOUT:
            logger.warning(f'Incorrect library_layout for {run_accession} having one fastq file')
            return False
        if '_1.f' in sorted_fastq[0] or '_2.f' in sorted_fastq[0]:
            logger.warning(f'Single fastq file contains _1 or _2 for run {run_accession}')
            return False
        else:
            return sorted_fastq
    # potential paired end
    elif len(sorted_fastq) == 2:
        if library_layout == SINGLE_END_LIBRARY_LAYOUT:
            logger.warning(f'Incorrect library_layout for {run_accession} having two fastq files')
            return False
        if '_1.f' in sorted_fastq[0] and '_2.f' in sorted_fastq[1]:
            return sorted_fastq
        else:
            logger.warning(f"Incorrect names of fastq files for run {run_accession} (${sorted_fastq})")
            return False
    elif len(fastq) > 2:
        logger.info(f'More than 2 fastq files provided for run {run_accession}')
        return sorted_fastq[:2]


@task(
    retries=10,
    cache_key_fn=context_agnostic_task_input_hash,
    retry_delay_seconds=60,
    task_run_name="Get study readruns from ENA: {accession}",
    log_prints=True,
)
def get_study_readruns_from_ena(
    accession: str, limit: int = 20, filter_library_strategy: str = None
) -> List[str]:
    logger = get_run_logger()

    # api call arguments
    query = f'"(study_accession={accession} OR secondary_study_accession={accession})"'
    if filter_library_strategy:
        query = f'{query[:-1]} AND library_strategy={filter_library_strategy}"'
    query = query.replace('"', "%22")

    fields = ",".join(EMG_CONFIG.ena.readrun_metadata_fields)
    result_type = "read_run"

    logger.info(f"Will fetch study {accession} read-runs from ENA portal API")

    mgys_study = analyses.models.Study.objects.get(ena_study__accession=accession)

    portal = httpx.get(
        create_ena_api_request(
            result_type=result_type,
            query=query,
            limit=limit,
            fields=fields
        ) + '&dataPortal=metagenome'
    )
    if not portal.status_code == httpx.codes.OK:
        raise Exception(f"Bad status! {portal.status_code} {portal}")

    logger.info("ENA portal responded ok.")
    for read_run in portal.json():
        # check scientific name and metagenome source
        if METAGENOME_SCIENTIFIC_NAME not in read_run['scientific_name'] and \
            read_run['library_source'] not in ALLOWED_LIBRARY_SOURCE:
            logger.warning(f"Run {read_run['run_accession']} is not in metagenome taxa and not in allowed library_sources. "
                               f"No further processing for that run.")
            continue

        # check fastq files order/presence
        fastq_ftp_reads = check_reads_fastq(fastq=read_run["fastq_ftp"].split(";"), run_accession=read_run["run_accession"],
                                      library_layout=read_run["library_layout"])
        if not fastq_ftp_reads:
            logger.warning('Incorrect structure of fastq files provided. No further processing for that run.')
            continue

        logger.info(f"Creating objects for {read_run['run_accession']}")
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

        run, _ = analyses.models.Run.objects.update_or_create(
            ena_accessions=[read_run["run_accession"]],
            study=mgys_study,
            ena_study=mgys_study.ena_study,
            sample=mgnify_sample,
            defaults={
                "metadata": {
                    "library_strategy": read_run["library_strategy"],
                    "library_layout": read_run["library_layout"],
                    "library_source": read_run["library_source"],
                    "scientific_name": read_run["scientific_name"],
                    "fastq_ftps": fastq_ftp_reads,
                }
            },
        )
        run.set_experiment_type_by_ena_library_strategy(read_run["library_strategy"])

    mgys_study.refresh_from_db()
    return [run.ena_accessions[0] for run in mgys_study.runs.all()]
