from django.urls import path

from . import views

app_name = "workflows"

urlpatterns = [
    path(
        "edit-samplesheet/fetch/<str:filepath_encoded>/",
        views.edit_samplesheet_fetch_view,
        name="edit_samplesheet_fetch",
    ),
    path(
        "edit-samplesheet/edit/<str:filepath_encoded>/",
        views.edit_samplesheet_edit_view,
        name="edit_samplesheet_edit",
    ),
]
