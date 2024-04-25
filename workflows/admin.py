from django.contrib import admin

from .models import PrefectInitiatedHPCJob

from unfold.admin import ModelAdmin


@admin.register(PrefectInitiatedHPCJob)
class StudyAdmin(ModelAdmin):
    ...
