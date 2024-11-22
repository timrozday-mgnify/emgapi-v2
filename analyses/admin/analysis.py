from typing import Iterable

from django.contrib import admin
from django.db.models import Q
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from unfold.admin import ModelAdmin
from unfold.decorators import action

from analyses.admin.base import StatusListFilter, StudyFilter
from analyses.models import Analysis, Run


class AnalysisStatusListFilter(StatusListFilter):
    def get_statuses(self) -> Iterable[str]:
        return Analysis.AnalysisStates


class ExperimentTypeListFilter(admin.SimpleListFilter):
    title = "experiment type"
    parameter_name = "experiment_type"

    def lookups(self, request, model_admin):
        return Run.ExperimentTypes.choices

    def queryset(self, request, queryset):
        if self.value() is None:
            return queryset

        if self.value() in [
            Run.ExperimentTypes.ASSEMBLY,
            Run.ExperimentTypes.LONG_READ_ASSEMBLY,
            Run.ExperimentTypes.HYBRID_ASSEMBLY,
        ]:
            # TODO: this isn't accurate; maybe add an experiment type field to analysis model instead
            return queryset.filter(assembly__isnull=False)

        return queryset.filter(
            Q(run__experiment_type=self.value())
            | Q(assembly__run__experiment_type=self.value())
        )


@admin.register(Analysis)
class AnalysisAdmin(ModelAdmin):
    list_display = [
        "__str__",
        "assembly_or_run",
        "created_at",
        "updated_at",
        "study",
        "status_summary",
    ]

    search_fields = [
        "id",
        "run__ena_accessions",
        "assembly__ena_accessions",
        "assembly__run__ena_accessions",
        "ena_study__title",
        "ena_study__accession",
        "ena_study__additional_accessions",
        "study__accession",
        "study__title",
        "study__ena_accessions",
        "pipeline_version",
    ]

    readonly_fields = ["created_at", "accession"]

    fieldsets = (
        (None, {"fields": ["accession"]}),
        (
            "Analysed data",
            {
                "classes": ["tab"],
                "fields": ["ena_study", "study", "sample", "run", "assembly"],
            },
        ),
        (
            "Status and ownership",
            {
                "classes": ["tab"],
                "fields": ["is_ready", "is_private", "webin_submitter", "status"],
            },
        ),
        ("Files", {"classes": ["tab"], "fields": ["downloads", "results_dir"]}),
    )

    class StudyFilterForAnalysis(StudyFilter):
        study_accession_search_fields = [
            "ena_study__accession",
            "study__accession",
        ]

    list_filter = [
        StudyFilterForAnalysis,
        "pipeline_version",
        ExperimentTypeListFilter,
        "updated_at",
        "created_at",
        AnalysisStatusListFilter,
    ]
    list_filter_submit = True

    def status_summary(self, obj):
        if (not obj.status) or (not type(obj.status) is dict):
            return None
        return " — ".join(
            [
                status.upper()
                for status, is_set in obj.status.items()
                if is_set and not status.endswith("reason")
            ]
        )

    actions_detail = ["get_annotations_json"]

    @action(
        description="Download: annotations JSON",
        url_path="analysis-annotations-json",
    )
    def get_annotations_json(self, request, object_id):
        analysis = get_object_or_404(Analysis, pk=object_id)
        response = JsonResponse(analysis.annotations)
        response["Content-Disposition"] = (
            f'attachment; filename="{analysis.accession}_annotations.json"'
        )
        return response
