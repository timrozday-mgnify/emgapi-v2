from django.contrib import admin
from unfold.admin import ModelAdmin

from .models import Sample, Study


@admin.register(Sample)
class SampleAdmin(ModelAdmin):
    pass


@admin.register(Study)
class StudyAdmin(ModelAdmin):
    pass
