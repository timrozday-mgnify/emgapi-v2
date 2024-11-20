from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.models import Sample


@admin.register(Sample)
class SampleAdmin(ModelAdmin):
    pass
