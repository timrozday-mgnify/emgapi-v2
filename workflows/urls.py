from django.urls import path

from . import views

app_name = "workflows"

urlpatterns = [
    path(
        "edit-samplesheet/<str:filepath_encoded>/",
        views.edit_samplesheet_view,
        name="edit_samplesheet",
    ),
]
