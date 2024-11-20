from django.contrib import admin
from unfold.admin import ModelAdmin
from unfold.decorators import display

from analyses.admin.base import StudyFilter
from analyses.models import Run


@admin.register(Run)
class RunAdmin(ModelAdmin):
    class StudyFilterForRun(StudyFilter):
        study_accession_search_fields = [
            "ena_study__accession",
            "study__accession",
            "study__ena_accessions",
        ]

    list_display = [
        "id",
        "display_accessions",
        "updated_at",
        "experiment_type",
        "study",
    ]
    list_filter = [StudyFilterForRun, "experiment_type", "updated_at"]
    list_filter_submit = True
    search_fields = [
        "ena_accessions",
        "experiment_type",
        "id",
        "ena_study__accession",
        "ena_study__additional_accessions",
        "sample__ena_accessions",
        "study__accession",
    ]

    @display(description="Accessions", label=True)
    def display_accessions(self, instance: Run):
        return instance.ena_accessions
