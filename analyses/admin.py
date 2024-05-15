from django.contrib import admin

from .models import Study, Sample, Analysis, AssemblyAnalysisRequest, Run

from unfold.admin import ModelAdmin


@admin.register(Study)
class StudyAdmin(ModelAdmin):
    pass


@admin.register(Sample)
class SampleAdmin(ModelAdmin):
    pass


@admin.register(Run)
class RunAdmin(ModelAdmin):
    pass


@admin.register(Analysis)
class AnalysisAdmin(ModelAdmin):
    pass


@admin.register(AssemblyAnalysisRequest)
class AssemblyAnalysisRequestAdmin(ModelAdmin):
    readonly_fields = ["flow_run_link"]
