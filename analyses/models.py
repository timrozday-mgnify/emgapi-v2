import os

from django.db import models
from django.db.models import F, Value, CharField, JSONField, AutoField
from django.db.models.functions import Concat, LPad, Cast

import ena.models


# Some models associated with MGnify Analyses (MGYS, MGYA etc).


class ConcatOp(models.Func):
    # TODO: remove after Django 5.1
    arg_joiner = " || "
    function = None
    output_field = models.TextField()
    template = "%(expressions)s"


class MGnifyAccessionedModel(models.Model):
    """
    Base class for models that are accessioned like MGYX001, with an auto ID and a zero-padded accession.
    Accessions are auto-generated, and persisted and indexed to the database.
    """

    id = AutoField(primary_key=True)
    accession_prefix = "MGY?"
    accession_length = 7
    accession = models.GeneratedField(
        expression=ConcatOp(
            Value(accession_prefix),
            LPad(Cast(F("id"), CharField()), accession_length, Value("0")),
        ),
        output_field=CharField(),
        db_persist=True,
        db_index=True,
    )
    is_ready = models.BooleanField(default=False)

    class Meta:
        abstract = True


class VisibilityControlledModel(models.Model):
    """
    Base class for models that should inherit their privacy status (so visibility) from an ENA Study.
    """

    is_private = models.BooleanField(default=False)
    ena_study = models.ForeignKey(ena.models.Study, on_delete=models.CASCADE)
    webin_submitter = CharField(null=True, blank=True, max_length=25, db_index=True)

    class Meta:
        abstract = True


# TODO: suppression propagation


class ENADerivedModel(VisibilityControlledModel):
    ena_accessions = JSONField(default=list, db_index=True)
    is_suppressed = models.BooleanField(default=False)

    class Meta:
        abstract = True


#         TODO – postgres GIN index on accessions?


class TimeStampedModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Study(MGnifyAccessionedModel, ENADerivedModel, TimeStampedModel):
    accession_prefix = "MGYS"
    accession_length = 8
    ena_study = models.ForeignKey(
        ena.models.Study, on_delete=models.CASCADE, null=True, blank=True
    )

    title = models.CharField(max_length=255)


class Sample(MGnifyAccessionedModel, ENADerivedModel, TimeStampedModel):
    ena_sample = models.ForeignKey(ena.models.Sample, on_delete=models.CASCADE)


class Analysis(MGnifyAccessionedModel, TimeStampedModel, VisibilityControlledModel):
    suppression_following_fields = ["sample"]

    study = models.ForeignKey(Study, on_delete=models.CASCADE)
    results_dir = models.CharField(max_length=100)
    sample = models.ForeignKey(
        Sample, on_delete=models.CASCADE, related_name="analyses"
    )


class Assembly(TimeStampedModel, ENADerivedModel):
    dir = models.CharField(max_length=200)


class AssemblyAnalysisRequest(TimeStampedModel):
    class AssemblyAnalysisStates:
        REQUEST_RECEIVED = "request_received"
        STUDY_FETCHED = "study_fetched"
        ASSEMBLY_STARTED = "assembly_started"
        ASSEMBLY_COMPLETED = "assembly_completed"
        ANALYSIS_STARTED = "analysis_started"
        ANALYSIS_COMPLETED = "analysis_completed"

        @classmethod
        def default_status(cls):
            return {
                cls.REQUEST_RECEIVED: True,
                cls.STUDY_FETCHED: False,
                cls.ASSEMBLY_STARTED: False,
                cls.ASSEMBLY_COMPLETED: False,
                cls.ANALYSIS_STARTED: False,
                cls.ANALYSIS_COMPLETED: False,
            }

    class RequestMetadata:
        STUDY_ACCESSION = "study_accession"
        FLOW_RUN_ID = "flow_run_id"

        @classmethod
        def default_metadata(cls):
            return {cls.STUDY_ACCESSION: None, cls.FLOW_RUN_ID: None}

    requestor = models.CharField(max_length=20)
    status = models.JSONField(
        default=AssemblyAnalysisStates.default_status, null=True, blank=True
    )
    study = models.ForeignKey(Study, on_delete=models.CASCADE, null=True, blank=True)
    request_metadata = models.JSONField(
        default=RequestMetadata.default_metadata, null=True, blank=True
    )

    @property
    def requested_study(self):
        return self.request_metadata.get(self.RequestMetadata.STUDY_ACCESSION)

    @property
    def flow_run_link(self):
        return f"{os.getenv('PREFECT_API_URL')}/flow-runs/flow-run/{self.request_metadata.get(self.RequestMetadata.FLOW_RUN_ID)}"

    def __str__(self):
        return f"AssemblyAnalysisRequest {self.pk}: {self.requested_study}"
