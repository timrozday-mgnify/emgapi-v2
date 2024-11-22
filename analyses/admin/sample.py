from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.admin.base import ENABrowserLinkMixin
from analyses.models import Sample


@admin.register(Sample)
class SampleAdmin(ENABrowserLinkMixin, ModelAdmin):
    pass
