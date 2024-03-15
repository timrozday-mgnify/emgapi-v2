from enum import Enum
from typing import Optional, List

from django.urls import reverse
from ninja import Schema, ModelSchema, Field

import analyses.models


class MGnifyStudy(ModelSchema):
    # accession: str
    # url: str
    # ena_study: Optional[str] = None

    # @staticmethod
    # def resolve_url(obj: analyses.models.Study) -> str:
    #     return reverse("api:get_mgnify_study", kwargs={"accession": obj.accession})

    class Meta:
        model = analyses.models.Study
        fields = ["accession", "ena_study"]
        fields_optional = ["ena_study"]


class MGnifySample(ModelSchema):
    class Meta:
        model = analyses.models.Sample
        fields = ["id", "ena_sample"]


class MGnifyAnalysis(ModelSchema):
    study_accession: str = Field(..., alias="study_id")

    class Meta:
        model = analyses.models.Analysis
        fields = ["accession", "results_dir"]


class MGnifyAnalysisTypedAnnotation(Schema):
    count: int
    description: str


class MGnifyAnalysisWithAnnotations(MGnifyAnalysis):
    annotations: dict[str, List[MGnifyAnalysisTypedAnnotation]]

    class Meta:
        model = analyses.models.Analysis
        fields = ["accession", "results_dir", "annotations"]


class MGnifyAssemblyAnalysisRequestCreate(ModelSchema):
    class Meta:
        model = analyses.models.AssemblyAnalysisRequest
        fields = ["requestor", "request_metadata"]


class MGnifyAssemblyAnalysisRequest(ModelSchema):
    class Meta:
        model = analyses.models.AssemblyAnalysisRequest
        fields = ["requestor", "status", "study", "request_metadata", "id"]


class MGnifyFunctionalAnalysisAnnotationType(Enum):
    genome_properties: str = analyses.models.Analysis.GENOME_PROPERTIES
    go_terms: str = analyses.models.Analysis.GO_TERMS
    go_slims: str = analyses.models.Analysis.GO_SLIMS
    interpro_identifiers: str = analyses.models.Analysis.INTERPRO_IDENTIFIERS
    kegg_modules: str = analyses.models.Analysis.KEGG_MODULES
    kegg_orthologs: str = analyses.models.Analysis.KEGG_ORTHOLOGS
    taxonomies: str = analyses.models.Analysis.TAXONOMIES
    antismash_gene_clusters: str = analyses.models.Analysis.ANTISMASH_GENE_CLUSTERS
    pfams: str = analyses.models.Analysis.PFAMS


class StudyAnalysisIntent(Schema):
    study_accession: str


# AnalysisSchema = create_model()


# class MGnifyAnalysisWithTypedAnnotations(Schema):
#     accession: str
#     annotations_list = List[MGnifyAnalysisTypedAnnotation]
