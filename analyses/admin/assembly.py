from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.admin.base import StudyFilter
from analyses.models import Assembler, Assembly


class AssemblyStatusListFilter(admin.SimpleListFilter):
    title = "status"

    parameter_name = "status"

    def lookups(self, request, model_admin):
        return [
            (state, state.replace("_", " ").title())
            for state in Assembly.AssemblyStates
        ]

    def queryset(self, request, queryset):
        if self.value() is None:
            return queryset
        return queryset.filter(**{f"status__{self.value()}": True})


@admin.register(Assembly)
class AssemblyAdmin(ModelAdmin):
    class StudyFilterForAssembly(StudyFilter):
        study_accession_search_fields = [
            "ena_study__accession",
            "reads_study__accession",
            "reads_study__ena_accessions",
            "assembly_study__accession",
            "assembly_study__ena_accessions",
        ]

    list_display = [
        "__str__",
        "created_at",
        "updated_at",
        "ena_study",
        "status_summary",
    ]
    list_filter = [
        StudyFilterForAssembly,
        "updated_at",
        "created_at",
        AssemblyStatusListFilter,
    ]
    list_filter_submit = True
    search_fields = [
        "id",
        "run__ena_accessions",
        "ena_study__title",
        "ena_study__accession",
        "ena_study__additional_accessions",
        "assembly_study__accession",
        "assembly_study__ena_study__accession",
        "assembly_study__ena_study__additional_accessions",
        "reads_study__accession",
        "reads_study__title",
        "reads_study__ena_study__accession",
        "reads_study__ena_study__additional_accessions",
    ]

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


@admin.register(Assembler)
class AssemblerAdmin(ModelAdmin):
    pass
