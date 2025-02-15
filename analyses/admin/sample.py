from django.contrib import admin
from unfold.admin import ModelAdmin

from analyses.admin.base import ENABrowserLinkMixin, JSONFieldWidgetOverridesMixin
from analyses.models import Sample


@admin.register(Sample)
class SampleAdmin(ENABrowserLinkMixin, JSONFieldWidgetOverridesMixin, ModelAdmin): ...
