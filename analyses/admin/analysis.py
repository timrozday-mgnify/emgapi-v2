from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.models import Analysis


@admin.register(Analysis)
class AnalysisAdmin(ModelAdmin):
    pass
