from django.contrib import admin
from unfold.admin import ModelAdmin

from workflows.models import OrchestratedClusterJob


@admin.register(OrchestratedClusterJob)
class OrchestratedClusterJobAdmin(ModelAdmin):
    search_fields = ["id", "cluster_job_id", "flow_run_id", "job_submit_description"]
    list_filter = [
        "last_known_state",
        "updated_at",
        "created_at",
    ]
