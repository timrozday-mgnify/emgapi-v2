import os

from django.db import models
from django.db.models import F, Value, CharField, JSONField, AutoField
from django.db.models.functions import LPad, Cast

import ena.models


# Some models associated with MGnify Analyses (MGYS, MGYA etc).


class ConcatOp(models.Func):
    # TODO: remove after Django 5.1
    arg_joiner = " || "
    function = None
    output_field = models.TextField()
    template = "%(expressions)s"


class MGnifyAccessionField(models.GeneratedField):
    accession_prefix: str = None
    accession_length: int = None

    """
    A special type of GeneratedField, that uses the models `id` to form a column of accessions.
    The accessions are prefixed (e.g. MGYA) and zero padded (e.g. MGYA000001).
    The accession are persisted to the DB (stored as a column) and unique and indexed (for use as lookups e.g. as a key).
    """

    def __init__(self, accession_prefix: str, accession_length: int, **kwargs):
        self.accession_prefix = accession_prefix
        self.accession_length = accession_length
        for dont_pass in [
            "expression",
            "output_field",
            "db_index",
            "db_persist",
            "unique",
        ]:
            kwargs.pop(dont_pass, None)

        super().__init__(
            expression=ConcatOp(
                Value(accession_prefix),
                LPad(Cast(F("id"), CharField()), accession_length, Value("0")),
            ),
            output_field=CharField(
                max_length=(accession_length + len(accession_prefix) + 2)
            ),
            db_persist=True,
            db_index=True,
            unique=True,
            **kwargs,
        )

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        kwargs["accession_prefix"] = self.accession_prefix
        kwargs["accession_length"] = self.accession_length
        return name, path, args, kwargs


class MGnifyAutomatedModel(models.Model):
    """
    Base class for models that have an autoincrementing ID (perhaps for use as an accession)
    and an `is_ready` bool for when they should appear on website vs. in automation only.
    """

    id = AutoField(primary_key=True)
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


class Study(MGnifyAutomatedModel, ENADerivedModel, TimeStampedModel):
    accession = MGnifyAccessionField(
        accession_prefix="MGYS", accession_length=8, db_index=True
    )
    ena_study = models.ForeignKey(
        ena.models.Study, on_delete=models.CASCADE, null=True, blank=True
    )

    title = models.CharField(max_length=255)


class Sample(MGnifyAutomatedModel, ENADerivedModel, TimeStampedModel):
    ena_sample = models.ForeignKey(ena.models.Sample, on_delete=models.CASCADE)


class AnalysisManagerDeferringAnnotations(models.Manager):
    """
    The annotations field is a potentially large JSONB field.
    Defer it by default, since most queries don't need to transfer this large dataset.
    """

    def get_queryset(self):
        return super().get_queryset().defer("annotations")


class AnalysisManagerIncludingAnnotations(models.Manager):
    def get_queryset(self):
        return super().get_queryset()


class Analysis(MGnifyAutomatedModel, TimeStampedModel, VisibilityControlledModel):
    objects = AnalysisManagerDeferringAnnotations()
    objects_and_annotations = AnalysisManagerIncludingAnnotations()

    accession = MGnifyAccessionField(accession_prefix="MGYA", accession_length=8)

    GENOME_PROPERTIES = "genome_properties"
    GO_TERMS = "go_terms"
    GO_SLIMS = "go_slims"
    INTERPRO_IDENTIFIERS = "interpro_identifiers"
    KEGG_MODULES = "kegg_modules"
    KEGG_ORTHOLOGS = "kegg_orthologs"
    TAXONOMIES = "taxonomies"
    ANTISMASH_GENE_CLUSTERS = "antismash_gene_clusters"
    PFAMS = "pfams"

    suppression_following_fields = ["sample"]
    study = models.ForeignKey(Study, on_delete=models.CASCADE, to_field="accession")
    results_dir = models.CharField(max_length=100)
    sample = models.ForeignKey(
        Sample, on_delete=models.CASCADE, related_name="analyses"
    )

    @staticmethod
    def default_annotations():
        return {
            Analysis.GENOME_PROPERTIES: [],
            Analysis.GO_TERMS: [],
            Analysis.GO_SLIMS: [],
            Analysis.INTERPRO_IDENTIFIERS: [],
            Analysis.KEGG_MODULES: [],
            Analysis.KEGG_ORTHOLOGS: [],
            Analysis.TAXONOMIES: [],
            Analysis.ANTISMASH_GENE_CLUSTERS: [],
            Analysis.PFAMS: [],
        }

    annotations = models.JSONField(default=default_annotations.__func__)

    class Meta:
        verbose_name_plural = "Analyses"


class AnalysedContig(TimeStampedModel):
    analysis = models.ForeignKey(Analysis, on_delete=models.CASCADE)
    contig_id = models.CharField(max_length=255)
    coverage = models.FloatField()
    length = models.IntegerField()

    PFAMS = "pfams"
    KEGGS = "keggs"
    INTERPROS = "interpros"
    COGS = "cogs"
    GOS = "gos"
    ANTISMASH_GENE_CLUSTERS = "antismash_gene_clusters"

    @staticmethod
    def default_annotations():
        return {
            AnalysedContig.PFAMS: [],
            AnalysedContig.KEGGS: [],
            AnalysedContig.INTERPROS: [],
            AnalysedContig.COGS: [],
            AnalysedContig.GOS: [],
            AnalysedContig.ANTISMASH_GENE_CLUSTERS: [],
        }

    annotations = models.JSONField(default=default_annotations.__func__)


class Assembly(TimeStampedModel, ENADerivedModel):
    dir = models.CharField(max_length=200)


class Run(TimeStampedModel, ENADerivedModel, MGnifyAutomatedModel):
    class CommonMetadataKeys:
        INSTRUMENT_PLATFORM = "instrument_platform"
        INSTRUMENT_MODEL = "instrument_model"

    class ExperimentTypes(models.TextChoices):
        METATRANSCRIPTOMIC = "METAT", "Metatranscriptomic"
        METAGENOMIC = "METAG", "Metagenomics"
        AMPLICON = "AMPLI", "Amplicon"
        ASSEMBLY = "ASSEM", "Assembly"
        HYBRID_ASSEMBLY = "HYASS", "Hybrid assembly"
        LONG_READ_ASSEMBLY = "LRASS", "Long-read assembly"

        # legacy
        METABARCODING = "METAB", "Metabarcoding"
        UNKNOWN = "UNKNO", "Unknown"

    experiment_type = models.CharField(
        choices=ExperimentTypes, max_length=5, default=ExperimentTypes.UNKNOWN
    )
    metadata = models.JSONField(default=dict)
    study = models.ForeignKey(Study, on_delete=models.CASCADE, related_name="runs")

    class RunStates:
        ASSEMBLY_STARTED = "assembly_started"
        ASSEMBLY_COMPLETED = "assembly_completed"
        ANALYSIS_STARTED = "analysis_started"
        ANALYSIS_COMPLETED = "analysis_completed"

        @classmethod
        def default_status(cls):
            return {
                cls.ASSEMBLY_STARTED: False,
                cls.ASSEMBLY_COMPLETED: False,
                cls.ANALYSIS_STARTED: False,
                cls.ANALYSIS_COMPLETED: False,
            }

    status = models.JSONField(default=RunStates.default_status, null=True, blank=True)

    async def mark_status(self, status: RunStates, set_status_as: bool = True):
        self.status[status] = set_status_as
        return self.asave()


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

    async def mark_status(
        self, status: AssemblyAnalysisStates, set_status_as: bool = True
    ):
        self.status[status] = set_status_as
        return self.asave()
