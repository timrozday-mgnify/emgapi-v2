import csv
import tempfile
from pathlib import Path

import pytest

import analyses.models
import ena.models
from workflows.nextflow_utils.samplesheets import queryset_to_samplesheet


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
        field_to_column_map={
            "id": "mgnify_sample_id",
            "ena_sample__accession": "ena_accession",
            "ena_study__accession": "ena_study_accession",
        },
    )

    with open(samplesheet_ret) as f:
        csv_reader = csv.DictReader(f)
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
            field_to_column_map={
                "id": "mgnify_sample_id",
                "ena_sample__accession": "ena_accession",
                "ena_study__accession": "ena_study_accession",
            },
        )
        assert "already exists" in str(e.value)

    # should work without exception if bludgeon is true
    queryset_to_samplesheet(
        queryset=qs,
        filename=samplesheet,
        field_to_column_map={
            "id": "mgnify_sample_id",
            "ena_sample__accession": "ena_accession",
            "ena_study__accession": "ena_study_accession",
        },
        bludgeon=True,
    )

    samplesheet_ret.unlink()

    # should succeed with default columns
    samplesheet_ret = queryset_to_samplesheet(queryset=qs, filename=samplesheet)
    with open(samplesheet_ret) as f:
        csv_reader = csv.DictReader(f)
        first_line = next(csv_reader)
        assert "updated_at" in first_line
        assert first_line["id"] == "1"

    samplesheet_ret.unlink(missing_ok=True)
