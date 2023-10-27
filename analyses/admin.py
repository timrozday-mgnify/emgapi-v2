from django.contrib import admin

from .models import Study, Sample, Analysis

admin.site.register(Study)
admin.site.register(Sample)
admin.site.register(Analysis)
