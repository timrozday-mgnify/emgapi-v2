from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.models import AssemblyAnalysisRequest


@admin.register(AssemblyAnalysisRequest)
class AssemblyAnalysisRequestAdmin(ModelAdmin):
    readonly_fields = ["flow_run_link"]
