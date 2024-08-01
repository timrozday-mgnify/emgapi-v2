import logging
import os
import re
from typing import ClassVar

from django.core.exceptions import (
    MultipleObjectsReturned,
    ObjectDoesNotExist,
    ValidationError,
)
from django.db import models
from django.db.models import Q, F, Value, CharField, JSONField, AutoField
from django.db.models.functions import LPad, Cast
from django_ltree.models import TreeModel

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
    ena_accessions = JSONField(default=list, db_index=True, blank=True)
    is_suppressed = models.BooleanField(default=False)

    @property
    def first_accession(self):
        if len(self.ena_accessions):
            return self.ena_accessions[0]
        return None

    class Meta:
        abstract = True


#         TODO – postgres GIN index on accessions?


class TimeStampedModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Biome(TreeModel):
    biome_name = models.CharField(max_length=255)

    def __str__(self):
        return self.pretty_lineage

    @property
    def pretty_lineage(self):
        return ":".join(self.ancestors().values_list("biome_name", flat=True))

    @property
    def descendants_count(self):
        return self.descendants().count()

    @staticmethod
    def lineage_to_path(lineage: str) -> str:
        """
        E.g. "root:Host-associated:Human:Digestive system:pañal" -> root.host-associated.human.digestive_system:paal
        :param lineage: Lineage string in colon-separated form.
        :return: Lineage as a dot-separated path suitable for a postgres ltree field (alphanumeric and _ only, nospaced)
        """
        ascii_lower = lineage.encode("ascii", "ignore").decode("ascii").lower()
        dot_separated = ascii_lower.replace(":", ".")
        underscore_punctuated = (
            dot_separated.replace(" ", "_")
            .replace("(", "_")
            .replace(")", "_")
            .replace("-", "_")
            .replace("__", "_")
            .strip("_.")
        )
        return re.sub(r"[^a-zA-Z0-9._]", "", underscore_punctuated)


class StudyManager(models.Manager):
    async def get_or_create_for_ena_study(self, ena_study_accession):
        logging.info(f"Will get/create MGnify study for {ena_study_accession}")
        try:
            ena_study = await ena.models.Study.objects.filter(
                Q(accession=ena_study_accession)
                | Q(additional_accessions__icontains=ena_study_accession)
            ).afirst()
            logging.debug(f"Got {ena_study}")
        except (MultipleObjectsReturned, ObjectDoesNotExist) as e:
            logging.warning(
                f"Problem getting ENA study {ena_study_accession} from ENA models DB"
            )
        study, _ = await Study.objects.aget_or_create(
            ena_study=ena_study, title=ena_study.title
        )
        return study


class Study(MGnifyAutomatedModel, ENADerivedModel, TimeStampedModel):
    accession = MGnifyAccessionField(
        accession_prefix="MGYS", accession_length=8, db_index=True
    )
    ena_study = models.ForeignKey(
        ena.models.Study, on_delete=models.CASCADE, null=True, blank=True
    )
    biome = models.ForeignKey(Biome, on_delete=models.CASCADE, null=True, blank=True)

    title = models.CharField(max_length=255)

    objects: StudyManager = StudyManager()

    def __str__(self):
        return self.accession

    class Meta:
        verbose_name_plural = "studies"


class Sample(MGnifyAutomatedModel, ENADerivedModel, TimeStampedModel):
    ena_sample = models.ForeignKey(ena.models.Sample, on_delete=models.CASCADE)

    def __str__(self):
        return f"Sample {self.id}: {self.ena_sample}"


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


class Run(TimeStampedModel, ENADerivedModel, MGnifyAutomatedModel):
    class CommonMetadataKeys:
        INSTRUMENT_PLATFORM = "instrument_platform"
        INSTRUMENT_MODEL = "instrument_model"

    class ExperimentTypes(models.TextChoices):
        METATRANSCRIPTOMIC = "METAT", "Metatranscriptomic"
        METAGENOMIC = "METAG", "Metagenomic"
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
    sample = models.ForeignKey(Sample, on_delete=models.CASCADE, related_name="runs")

    class RunStates:
        ANALYSIS_STARTED = "analysis_started"
        ANALYSIS_COMPLETED = "analysis_completed"

        @classmethod
        def default_status(cls):
            return {
                cls.ANALYSIS_STARTED: False,
                cls.ANALYSIS_COMPLETED: False,
            }

    status = models.JSONField(default=RunStates.default_status, null=True, blank=True)

    def mark_status(self, status: RunStates, set_status_as: bool = True):
        self.status[status] = set_status_as
        return self.save()

    def __str__(self):
        return f"Run {self.id}: {self.first_accession}"


class Assembler(TimeStampedModel):
    METASPADES = "metaspades"
    MEGAHIT = "megahit"
    SPADES = "spades"

    NAME_CHOICES = [
        (METASPADES, "MetaSPAdes"),
        (MEGAHIT, "MEGAHIT"),
        (SPADES, "SPAdes"),
    ]

    assembler_default: ClassVar[str] = METASPADES

    name = models.CharField(max_length=20, null=True, blank=True, choices=NAME_CHOICES)
    version = models.CharField(max_length=20)

    def save(self, *args, **kwargs):
        self.clean()
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.name} {self.version}" if self.version is not None else self.name


class AssemblyManager(models.Manager):
    def get_queryset(self):
        return super().get_queryset().select_related("run")


class Assembly(TimeStampedModel, ENADerivedModel):
    objects = AssemblyManager()

    dir = models.CharField(max_length=200, null=True, blank=True)
    run = models.ForeignKey(
        Run, on_delete=models.CASCADE, related_name="assemblies", null=True, blank=True
    )
    # raw reads study that was used as resource for assembly
    reads_study = models.ForeignKey(
        Study,
        on_delete=models.CASCADE,
        related_name="assemblies_reads",
        null=True,
        blank=True,
    )
    # TPA study that was created to submit assemblies
    assembly_study = models.ForeignKey(
        Study,
        on_delete=models.CASCADE,
        related_name="assemblies_assembly",
        null=True,
        blank=True,
    )
    assembler = models.ForeignKey(
        Assembler,
        on_delete=models.CASCADE,
        related_name="assemblies",
        null=True,
        blank=True,
    )
    # coverage,...
    metadata = JSONField(default=list, db_index=True, blank=True)

    class AssemblyStates:
        ENA_METADATA_SANITY_CHECK_FAILED = "ena_metadata_sanity_check_failed"
        ENA_DATA_QC_CHECK_FAILED = "ena_data_qc_check_failed"
        ASSEMBLY_STARTED = "assembly_started"
        POST_ASSEMBLY_QC_FAILED = "post_assembly_qc_failed"
        ASSEMBLY_FAILED = "assembly_failed"
        ASSEMBLY_COMPLETED = "assembly_completed"
        ASSEMBLY_BLOCKED = "assembly_blocked"
        ASSEMBLY_UPLOADED = "assembly_uploaded"
        ASSEMBLY_UPLOAD_FAILED = "assembly_upload_failed"
        ASSEMBLY_UPLOAD_BLOCKED = "assembly_upload_blocked"
        ANALYSIS_STARTED = "analysis_started"
        ANALYSIS_COMPLETED = "analysis_completed"

        @classmethod
        def default_status(cls):
            return {
                cls.ASSEMBLY_STARTED: False,
                cls.ASSEMBLY_FAILED: False,
                cls.ASSEMBLY_COMPLETED: False,
                cls.ASSEMBLY_BLOCKED: False,
                cls.ANALYSIS_STARTED: False,
                cls.ANALYSIS_COMPLETED: False,
                cls.ASSEMBLY_UPLOADED: False,
                cls.ASSEMBLY_UPLOAD_FAILED: False,
                cls.ASSEMBLY_UPLOAD_BLOCKED: False,
            }

    status = models.JSONField(
        default=AssemblyStates.default_status, null=True, blank=True
    )

    def mark_status(self, status: AssemblyStates, set_status_as: bool = True):
        self.status[status] = set_status_as
        return self.save()

    def add_erz_accession(self, erz_accession):
        if erz_accession not in self.ena_accessions:
            self.ena_accessions.append(erz_accession)
            return self.save()

    class Meta:
        verbose_name_plural = "Assemblies"

        constraints = [
            models.CheckConstraint(
                check=Q(reads_study__isnull=False) | Q(assembly_study__isnull=False),
                name="at_least_one_study_present",
            )
        ]

    def __str__(self):
        return f"Assembly {self.id}  (Run {self.run.first_accession})"


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

    def mark_status(self, status: AssemblyAnalysisStates, set_status_as: bool = True):
        self.status[status] = set_status_as
        return self.save()


class ComputeResourceHeuristic(TimeStampedModel):
    """
    Model for heuristics like how much memory is needed to assemble a certain biome with a certain assembler.
    """

    # process type for when the heuristic should be used
    class ProcessTypes(models.TextChoices):
        ASSEMBLY = "ASSEM", "Assembly"

    process = models.CharField(
        choices=ProcessTypes, max_length=5, null=True, blank=True
    )

    # relationships used for selecting heuristic value
    biome = models.ForeignKey(Biome, on_delete=models.CASCADE, null=True, blank=True)
    assembler = models.ForeignKey(
        Assembler, on_delete=models.CASCADE, null=True, blank=True
    )

    # heuristic values
    memory_gb = models.FloatField(null=True, blank=True)

    def __str__(self):
        if self.process == self.ProcessTypes.ASSEMBLY:
            return f"ComputeResourceHeuristic {self.id} (Use {self.memory_gb:.0f} GB to assemble {self.biome} with {self.assembler})"
        else:
            return f"ComputeResourceHeuristic {self.id ({self.process})}"
