from __future__ import annotations

import base64
import csv
import logging
from pathlib import Path

from django.contrib.admin.views.decorators import staff_member_required
from django.http import Http404
from django.shortcuts import redirect, render
from django.urls import reverse
from unfold.sites import UnfoldAdminSite

from emgapiv2.settings import EMG_CONFIG
from workflows.nextflow_utils.samplesheets import (
    location_for_samplesheet_to_be_edited,
    location_where_samplesheet_was_edited,
    move_samplesheet_back_from_editable_location,
    move_samplesheet_to_editable_location,
)

logger = logging.getLogger(__name__)


def encode_samplesheet_path(filepath: str | Path) -> str:
    return base64.urlsafe_b64encode(str(filepath).encode("utf-8")).decode("utf-8")


def validate_samplesheet_path(filepath_encoded: str) -> Path:
    try:
        _filepath = base64.urlsafe_b64decode(filepath_encoded).decode("utf-8")
    except (TypeError, ValueError):
        raise Http404("Invalid file path: couldn't decode")
    else:
        filepath = Path(_filepath)

    if not filepath.is_absolute():
        raise Http404("Invalid file path: not absolute")

    if not filepath.suffix.lower() in [".csv", ".tsv"]:
        raise Http404(f"Invalid file type: {filepath.suffix.lower()}")

    if not filepath.is_relative_to(
        Path(EMG_CONFIG.slurm.samplesheet_editing_allowed_inside).resolve()
    ):
        raise Http404("Invalid directory")

    return filepath


@staff_member_required
def edit_samplesheet_fetch_view(request, filepath_encoded: str):
    filepath = validate_samplesheet_path(filepath_encoded)

    # asked for a samplesheet in e.g. /nfs/production
    # start copying it to editable location

    editable_location = move_samplesheet_to_editable_location(filepath, timeout=0)

    logger.info(f"File will be moved to {editable_location}")

    url = reverse(
        "workflows:edit_samplesheet_edit", kwargs={"filepath_encoded": filepath_encoded}
    )
    logger.info(f"Will redirect to {url}")
    return redirect(url)


@staff_member_required
def edit_samplesheet_edit_view(request, filepath_encoded: str):
    filepath = validate_samplesheet_path(filepath_encoded)

    destination_where_editable_inbound = location_for_samplesheet_to_be_edited(
        filepath, EMG_CONFIG.slurm.shared_filesystem_root_on_server
    )

    if request.method == "POST":
        csv_data = request.POST.get("csv_data", "")
        csv_lines = csv_data.splitlines()
        csv_reader = csv.reader(csv_lines)

        destination_where_editable_outbound = location_where_samplesheet_was_edited(
            filepath,
            EMG_CONFIG.slurm.shared_filesystem_root_on_server,
        )

        with open(destination_where_editable_outbound, "w", newline="") as csvfile:
            csv_writer = csv.writer(csvfile)
            for row in csv_reader:
                if any(row):  # do not write empty rows
                    csv_writer.writerow(row)

        move_samplesheet_back_from_editable_location(filepath, timeout=0)

        return redirect("admin:index")

    unfold_context = UnfoldAdminSite().each_context(request)

    if not destination_where_editable_inbound.is_file():
        # assume copying has not completed yet
        return render(
            request,
            "workflows/edit_samplesheet.html",
            {
                "filename": filepath,
                "csv_data": None,
                "loading": True,
                **unfold_context,
            },
        )

    assert destination_where_editable_inbound.is_file()

    with open(destination_where_editable_inbound, "r") as csvfile:
        csv_reader = csv.reader(csvfile)
        csv_content = list(csv_reader)

    csv_data = "\n".join([",".join(row) for row in csv_content])

    return render(
        request,
        "workflows/edit_samplesheet.html",
        {
            "filename": filepath,
            "csv_data": csv_data,
            "loading": False,
            **unfold_context,
        },
    )
