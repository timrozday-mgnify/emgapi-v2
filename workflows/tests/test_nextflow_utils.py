import csv
import tempfile
from pathlib import Path

import pytest

import analyses.models
import ena.models
from workflows.nextflow_utils.samplesheets import (
    queryset_to_samplesheet,
    queryset_hash,
    SamplesheetColumnSource,
)


@pytest.mark.django_db
def test_queryset_to_samplesheet(mgnify_study):
    for i in range(10):
        ena_sample = ena.models.Sample.objects.create(
            study=mgnify_study.ena_study, accession=f"SAM{i}"
        )
        analyses.models.Sample.objects.create(
            ena_sample=ena_sample, ena_study=mgnify_study.ena_study
        )

    qs = analyses.models.Sample.objects.all()

    # should fail if bad path given
    with pytest.raises(Exception) as e:
        queryset_to_samplesheet(
            queryset=qs, filename=tempfile.gettempdir() + "/not-a-folder"
        )
        assert "not-a-folder does not exist" in str(e.value)

    samplesheet = Path(tempfile.gettempdir()) / Path("samplesheet_test.csv")

    # should succeed with specified columns
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
        csv_reader = csv.DictReader(f, delimiter="\t")
        assert next(csv_reader) == {
            "mgnify_sample_id": "1",
            "ena_accession": "SAM0",
            "ena_study_accession": "PRJ1",
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
        csv_reader = csv.DictReader(f, delimiter="\t")
        first_line = next(csv_reader)
        assert "updated_at" in first_line
        assert first_line["id"] == "1"

    samplesheet_ret.unlink(missing_ok=True)

    # should use renderer function if given, e.g. for a json field
    sample = qs.first()
    run = analyses.models.Run.objects.create(
        sample=sample,
        ena_study=sample.ena_study,
        study=mgnify_study,
        metadata={
            "fastqs": ["/path/to/fastq_1.fastq.gz", "/path/to/fastq_2.fastq.gz"],
        },
    )

    run_qs = analyses.models.Run.objects.filter(id=run.id)

    samplesheet_ret = queryset_to_samplesheet(
        queryset=run_qs,
        filename=samplesheet,
        column_map={
            "fastq1": SamplesheetColumnSource(
                lookup_string="metadata__fastqs", renderer=lambda f: f[0]
            ),
            "fastq2": SamplesheetColumnSource(
                lookup_string="metadata__fastqs", renderer=lambda f: f[1]
            ),
        },
    )
    with open(samplesheet_ret) as f:
        csv_reader = csv.DictReader(f, delimiter="\t")
        first_line = next(csv_reader)
        assert first_line["fastq1"] == "/path/to/fastq_1.fastq.gz"
        assert first_line["fastq2"] == "/path/to/fastq_2.fastq.gz"
    samplesheet_ret.unlink(missing_ok=True)


@pytest.mark.django_db
def test_queryset_hash(mgnify_study):
    studies = analyses.models.Study.objects.all()
    hash = queryset_hash(studies, "ena_study__title")
    assert hash == "3b387536d51e5c045256364275533aa4"  # md5 of "Project 1"
