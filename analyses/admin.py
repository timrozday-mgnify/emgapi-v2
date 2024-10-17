import json

from django import forms
from django.contrib import admin
from django.db import models
from unfold.admin import ModelAdmin, TabularInline

from emgapiv2.widgets import StatusPathwayWidget

from .models import (
    Analysis,
    Assembler,
    Assembly,
    AssemblyAnalysisRequest,
    Biome,
    ComputeResourceHeuristic,
    Run,
    Sample,
    Study,
)


class StudyRunsInline(TabularInline):
    model = Run
    show_change_link = True
    fields = [
        "first_accession",
        "experiment_type",
        "sample",
        "latest_analysis_status_display",
    ]
    readonly_fields = ["first_accession", "latest_analysis_status_display"]
    max_num = 0

    def latest_analysis_status_display(self, obj: Run):
        if obj.latest_analysis_status:
            widget = StatusPathwayWidget(
                pathway=[
                    Analysis.AnalysisStates.ANALYSIS_STARTED,
                    Analysis.AnalysisStates.ANALYSIS_COMPLETED,
                ]
            )
            return widget.render(
                "latest_analysis_status", json.dumps(obj.latest_analysis_status)
            )
        return "No Status"


class StudyAssembliesInline(TabularInline):
    model = Assembly
    show_change_link = True
    fields = ["run", "status", "dir"]
    readonly_fields = ["run"]
    max_num = 0
    fk_name = "assembly_study"
    formfield_overrides = {
        models.JSONField: {
            "widget": StatusPathwayWidget(
                pathway=[
                    Assembly.AssemblyStates.ASSEMBLY_STARTED,
                    Assembly.AssemblyStates.ASSEMBLY_COMPLETED,
                    Assembly.AssemblyStates.ASSEMBLY_FAILED,
                    Assembly.AssemblyStates.ASSEMBLY_BLOCKED,
                    Assembly.AssemblyStates.ASSEMBLY_UPLOADED,
                    Assembly.AssemblyStates.ASSEMBLY_UPLOAD_FAILED,
                    Assembly.AssemblyStates.ASSEMBLY_UPLOAD_BLOCKED,
                ]
            )
        },
    }


class StudyReadsInline(TabularInline):
    model = Assembly
    show_change_link = True
    fields = ["run", "status", "dir"]
    readonly_fields = ["run"]
    max_num = 0
    fk_name = "reads_study"
    formfield_overrides = {
        models.JSONField: {
            "widget": StatusPathwayWidget(
                pathway=[
                    Assembly.AssemblyStates.ASSEMBLY_STARTED,
                    Assembly.AssemblyStates.ASSEMBLY_COMPLETED,
                    Assembly.AssemblyStates.ASSEMBLY_FAILED,
                    Assembly.AssemblyStates.ASSEMBLY_BLOCKED,
                    Assembly.AssemblyStates.ASSEMBLY_UPLOADED,
                    Assembly.AssemblyStates.ASSEMBLY_UPLOAD_FAILED,
                    Assembly.AssemblyStates.ASSEMBLY_UPLOAD_BLOCKED,
                ]
            )
        },
    }


@admin.register(Study)
class StudyAdmin(ModelAdmin):
    inlines = [StudyRunsInline, StudyAssembliesInline, StudyReadsInline]
    list_display = ["accession", "updated_at", "title", "ena_study"]
    list_filter = ["updated_at", "created_at"]
    search_fields = [
        "accession",
        "title",
        "ena_study__title",
        "ena_study__accession",
        "ena_study__additional_accessions",
        "biome__biome_name",
    ]


@admin.register(Sample)
class SampleAdmin(ModelAdmin):
    pass


@admin.register(Run)
class RunAdmin(ModelAdmin):
    pass


@admin.register(Assembly)
class AssemblyAdmin(ModelAdmin):
    pass


@admin.register(Analysis)
class AnalysisAdmin(ModelAdmin):
    pass


@admin.register(AssemblyAnalysisRequest)
class AssemblyAnalysisRequestAdmin(ModelAdmin):
    readonly_fields = ["flow_run_link"]


@admin.register(Assembler)
class AssemblerAdmin(ModelAdmin):
    pass


class BiomeForm(forms.ModelForm):
    parent = forms.ModelChoiceField(queryset=Biome.objects.all(), required=True)

    class Meta:
        model = Biome
        fields = ["biome_name", "parent"]

    def save_m2m(self):
        return self._save_m2m()

    def save(self, commit=True):
        parent = self.cleaned_data.get("parent")
        if not parent:
            parent = Biome.objects.roots.first()
        return Biome.objects.create_child(
            biome_name=self.cleaned_data["biome_name"], parent=parent
        )


@admin.register(Biome)
class BiomeAdmin(ModelAdmin):
    form = BiomeForm
    readonly_fields = ["pretty_lineage", "descendants_count"]
    search_fields = ["path", "biome_name"]


@admin.register(ComputeResourceHeuristic)
class ComputeResourceHeuristicAdmin(ModelAdmin):
    search_fields = ["biome__path", "assembler__name", "process"]
    list_filter = [
        "assembler",
        "process",
    ]
