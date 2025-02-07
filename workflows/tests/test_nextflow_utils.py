import csv
import tempfile
from pathlib import Path

import pytest

import analyses.models
import ena.models
from workflows.flows.hello_nextflow import hello_nextflow
from workflows.models import OrchestratedClusterJob
from workflows.nextflow_utils.samplesheets import (
    SamplesheetColumnSource,
    queryset_hash,
    queryset_to_samplesheet,
)
from workflows.prefect_utils.testing_utils import run_flow_and_capture_logs


@pytest.mark.django_db(transaction=True, reset_sequences=True)
def test_queryset_to_samplesheet(raw_reads_mgnify_study):
    for i in range(10):
        ena_sample = ena.models.Sample.objects.create(
            study=raw_reads_mgnify_study.ena_study, accession=f"SAM{i}"
        )
        analyses.models.Sample.objects.create(
            ena_sample=ena_sample, ena_study=raw_reads_mgnify_study.ena_study
        )

    qs = analyses.models.Sample.objects.all()

    # should fail if bad path given
    with pytest.raises(Exception) as e:
        queryset_to_samplesheet(
            queryset=qs, filename=tempfile.gettempdir() + "/not-a-folder"
        )
        assert "not-a-folder does not exist" in str(e.value)

    samplesheet = Path(tempfile.gettempdir()) / Path("samplesheet_test.tsv")

    # should succeed with specified columns as TSV
    samplesheet_tsv_ret = queryset_to_samplesheet(
        queryset=qs,
        filename=samplesheet,
        column_map={
            "mgnify_sample_id": SamplesheetColumnSource(lookup_string="id"),
            "ena_accession": SamplesheetColumnSource(
                lookup_string="ena_sample__accession"
            ),
            "ena_study_accession": SamplesheetColumnSource(
                lookup_string="ena_study__accession"
            ),
        },
    )

    with open(samplesheet_tsv_ret) as f:
        csv_reader = csv.DictReader(f, delimiter="\t")
        assert next(csv_reader) == {
            "mgnify_sample_id": "1",
            "ena_accession": "SAM0",
            "ena_study_accession": raw_reads_mgnify_study.ena_study.accession,
        }

    # should succeed with specified columns as CSV
    samplesheet = Path(tempfile.gettempdir()) / Path("samplesheet_test.csv")

    samplesheet_ret = queryset_to_samplesheet(
        queryset=qs,
        filename=samplesheet,
        column_map={
            "mgnify_sample_id": SamplesheetColumnSource(lookup_string="id"),
            "ena_accession": SamplesheetColumnSource(
                lookup_string="ena_sample__accession"
            ),
            "ena_study_accession": SamplesheetColumnSource(
                lookup_string="ena_study__accession"
            ),
        },
    )

    with open(samplesheet_ret) as f:
        csv_reader = csv.DictReader(f, delimiter=",")
        assert next(csv_reader) == {
            "mgnify_sample_id": "1",
            "ena_accession": "SAM0",
            "ena_study_accession": raw_reads_mgnify_study.ena_study.accession,
        }

    # should now fail because file exists
    with pytest.raises(Exception) as e:
        queryset_to_samplesheet(
            queryset=qs,
            filename=samplesheet,
            column_map={
                "mgnify_sample_id": SamplesheetColumnSource(lookup_string="id"),
                "ena_accession": SamplesheetColumnSource(
                    lookup_string="ena_sample__accession"
                ),
                "ena_study_accession": SamplesheetColumnSource(
                    lookup_string="ena_study__accession"
                ),
            },
        )
        assert "already exists" in str(e.value)

    # should work without exception if bludgeon is true
    queryset_to_samplesheet(
        queryset=qs,
        filename=samplesheet,
        column_map={
            "mgnify_sample_id": SamplesheetColumnSource(lookup_string="id"),
            "ena_accession": SamplesheetColumnSource(
                lookup_string="ena_sample__accession"
            ),
            "ena_study_accession": SamplesheetColumnSource(
                lookup_string="ena_study__accession"
            ),
        },
        bludgeon=True,
    )

    samplesheet_ret.unlink()

    # should succeed with default columns
    samplesheet_ret = queryset_to_samplesheet(queryset=qs, filename=samplesheet)
    with open(samplesheet_ret) as f:
        csv_reader = csv.DictReader(f, delimiter=",")
        first_line = next(csv_reader)
        assert "updated_at" in first_line
        assert first_line["id"] == "1"

    samplesheet_ret.unlink(missing_ok=True)

    # should use renderer function if given, e.g. for a json field
    sample = qs.first()
    run = analyses.models.Run.objects.create(
        sample=sample,
        ena_study=sample.ena_study,
        study=raw_reads_mgnify_study,
        metadata={
            analyses.models.Run.CommonMetadataKeys.FASTQ_FTPS: [
                "/path/to/fastq_1.fastq.gz",
                "/path/to/fastq_2.fastq.gz",
            ],
        },
    )

    run_qs = analyses.models.Run.objects.filter(id=run.id)

    samplesheet_ret = queryset_to_samplesheet(
        queryset=run_qs,
        filename=samplesheet,
        column_map={
            "fastq1": SamplesheetColumnSource(
                lookup_string="metadata__fastq_ftps", renderer=lambda f: f[0]
            ),
            "fastq2": SamplesheetColumnSource(
                lookup_string="metadata__fastq_ftps", renderer=lambda f: f[1]
            ),
        },
    )
    with open(samplesheet_ret) as f:
        csv_reader = csv.DictReader(f, delimiter=",")
        first_line = next(csv_reader)
        assert first_line["fastq1"] == "/path/to/fastq_1.fastq.gz"
        assert first_line["fastq2"] == "/path/to/fastq_2.fastq.gz"
    samplesheet_ret.unlink(missing_ok=True)


@pytest.mark.django_db(transaction=True)
def test_queryset_hash(raw_reads_mgnify_study):
    studies = analyses.models.Study.objects.all()
    hash = queryset_hash(studies, "ena_study__title")
    assert hash == "3b387536d51e5c045256364275533aa4"  # md5 of "Project 1"


@pytest.mark.django_db(transaction=True)
def test_nextflow_trace_from_flag(prefect_harness):
    # It should capture the logs either from the trace specified path
    hello_nextfow_flow_with_flag = run_flow_and_capture_logs(
        hello_nextflow, with_trace_flag=True
    )
    assert "Trace file -" in hello_nextfow_flow_with_flag.logs

    job1: OrchestratedClusterJob = OrchestratedClusterJob.objects.get(
        flow_run_id=hello_nextfow_flow_with_flag.flow_run_id
    )
    assert job1.nextflow_trace is not None

    # of from the specified path in the config file
    hello_nextfow_flow_with_config = run_flow_and_capture_logs(
        hello_nextflow, with_trace_flag=False
    )
    assert "Trace file -" in hello_nextfow_flow_with_config.logs

    job2: OrchestratedClusterJob = OrchestratedClusterJob.objects.get(
        flow_run_id=hello_nextfow_flow_with_config.flow_run_id
    )
    assert job2.nextflow_trace is not None
