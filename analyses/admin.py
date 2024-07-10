from django.contrib import admin
from django.db import models

from emgapiv2.widgets import StatusPathwayWidget
from .models import Study, Sample, Analysis, AssemblyAnalysisRequest, Run, Assembly

from unfold.admin import ModelAdmin, TabularInline


class StudyRunsInline(TabularInline):
    model = Run
    show_change_link = True
    fields = ["first_accession", "experiment_type", "sample", "status"]
    readonly_fields = ["first_accession"]
    max_num = 0
    formfield_overrides = {
        models.JSONField: {
            "widget": StatusPathwayWidget(
                pathway=[
                    Run.RunStates.ANALYSIS_STARTED,
                    Run.RunStates.ANALYSIS_COMPLETED,
                ]
            )
        },
    }


class StudyAssembliesInline(TabularInline):
    model = Assembly
    show_change_link = True
    fields = ["run", "status", "dir"]
    readonly_fields = ["run"]
    max_num = 0
    fk_name = 'reads_study'
    formfield_overrides = {
        models.JSONField: {
            "widget": StatusPathwayWidget(
                pathway=[
                    Assembly.AssemblyStates.ASSEMBLY_STARTED,
                    Assembly.AssemblyStates.ASSEMBLY_COMPLETED,
                    Assembly.AssemblyStates.ASSEMBLY_FAILED,
                    Assembly.AssemblyStates.ASSEMBLY_BLOCKED,
                    Assembly.AssemblyStates.ANALYSIS_STARTED,
                    Assembly.AssemblyStates.ANALYSIS_COMPLETED,
                ]
            )
        },
    }


@admin.register(Study)
class StudyAdmin(ModelAdmin):
    inlines = [StudyRunsInline, StudyAssembliesInline]


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
