import logging
from enum import Enum
from typing import List, Optional

from django.http import Http404
from django.shortcuts import get_object_or_404
from ninja import NinjaAPI, ModelSchema, Schema
from ninja.pagination import RouterPaginated

import analyses.models

logger = logging.getLogger(__name__)

api = NinjaAPI(
    title="MGnify API",
    description="The API for [MGnify](https://www.ebi.ac.uk/metagenomics), "
    "EBI’s platform for the submission, analysis, discovery and comparison of metagenomic-derived datasets.",
    urls_namespace="api",
    csrf=True,
    version="2.0-alpha",
    default_router=RouterPaginated(),
)


class ApiSections(Enum):
    STUDIES = "Studies"
    SAMPLES = "Samples"
    ANALYSES = "Analyses"
    REQUESTS = "Requests"


class MGnifyStudy(ModelSchema):
    class Config:
        model = analyses.models.Study
        model_fields = ["accession", "ena_study"]


class MGnifySample(ModelSchema):
    class Config:
        model = analyses.models.Sample
        model_fields = ["accession", "ena_sample"]


class MGnifyAnalysis(ModelSchema):
    class Config:
        model = analyses.models.Analysis
        model_fields = ["accession", "study", "results_dir"]


class MGnifyAnalysisWithAnnotations(ModelSchema):
    class Config:
        model = analyses.models.Analysis
        model_fields = ["accession", "study", "results_dir", "annotations"]


class MGnifyAnalysisTypedAnnotation(Schema):
    count: int
    description: str


# class MGnifyAnalysisWithTypedAnnotations(Schema):
#     accession: str
#     annotations_list = List[MGnifyAnalysisTypedAnnotation]


class MGnifyAssemblyAnalysisRequestCreate(ModelSchema):
    class Config:
        model = analyses.models.AssemblyAnalysisRequest
        model_fields = ["requestor", "request_metadata"]


class MGnifyAssemblyAnalysisRequest(ModelSchema):
    class Config:
        model = analyses.models.AssemblyAnalysisRequest
        model_fields = ["requestor", "status", "study", "request_metadata", "id"]


@api.get(
    "/studies/{accession}",
    response=MGnifyStudy,
    tags=[ApiSections.STUDIES.value],
    summary="Get the detail of a single study analysed by MGnify",
    description="MGnify studies inherit directly from studies (or projects) in ENA.",
)
def get_mgnify_study(request, accession: str):
    study = get_object_or_404(analyses.models.Study, accession=accession)
    return study


@api.get(
    "/studies",
    response=List[MGnifyStudy],
    tags=[ApiSections.STUDIES.value],
    summary="List all studies analysed by MGnify",
    description="MGnify studies inherit directly from studies (or projects) in ENA.",
)
def list_mgnify_studies(request):
    qs = analyses.models.Study.objects.all()
    return qs


@api.get(
    "/analyses/{accession}",
    response=MGnifyAnalysis,
    tags=[ApiSections.ANALYSES.value],
)
def get_mgnify_analysis(request, accession: str):
    analysis = get_object_or_404(analyses.models.Analysis, accession=accession)
    return analysis


@api.get(
    "/analyses/{accession}/annotations",
    response=MGnifyAnalysisWithAnnotations,
    tags=[ApiSections.ANALYSES.value],
)
def get_mgnify_analysis_with_annotations(request, accession: str):
    analysis = get_object_or_404(analyses.models.Analysis, accession=accession)
    return analysis


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


@api.get(
    "/analyses/{accession}/annotations/{annotation_type}",
    response=List[MGnifyAnalysisTypedAnnotation],
    tags=[ApiSections.ANALYSES.value],
)
def get_mgnify_analysis_with_annotations_of_type(
    request,
    accession: str,
    annotation_type: MGnifyFunctionalAnalysisAnnotationType,
    limit: Optional[int] = None,
):
    try:
        annotations = (
            analyses.models.Analysis.objects.filter(accession=accession)
            .values_list(f"annotations__{annotation_type.value}", flat=True)
            .first()
        )
    except analyses.models.Analysis.DoesNotExist:
        raise Http404("No analysis found")

    if limit and annotations:
        return annotations[:limit]
    return annotations


@api.get(
    "/analyses",
    response=List[MGnifyAnalysis],
    tags=[ApiSections.ANALYSES.value],
)
def list_mgnify_analyses(request):
    qs = analyses.models.Analysis.objects.all()
    return qs


class StudyAnalysisIntent(Schema):
    study_accession: str


@api.get(
    "/analysis_requests",
    response=List[MGnifyAssemblyAnalysisRequest],
    tags=[ApiSections.REQUESTS.value],
)
def list_assembly_analysis_requests(request):
    qs = analyses.models.AssemblyAnalysisRequest.objects.all()
    # TODO: perms
    return qs


@api.get(
    "/analysis_requests/{id}",
    response=MGnifyAssemblyAnalysisRequest,
    tags=[ApiSections.REQUESTS.value],
)
def get_assembly_analysis_requests(request, analysis_request_id: int):
    return analyses.models.AssemblyAnalysisRequest.objects.get(id=analysis_request_id)
    # TODO: perms


@api.post(
    "/analysis_requests",
    response=MGnifyAssemblyAnalysisRequest,
    tags=[ApiSections.REQUESTS.value],
)
def create_assembly_analysis_request(
    request, payload: MGnifyAssemblyAnalysisRequestCreate
):
    assembly_analysis_request = analyses.models.AssemblyAnalysisRequest.objects.create(
        **payload.dict()
    )
    return assembly_analysis_request


# @api.post("/analyse")
# def create_study_analysis_intent(request, payload: StudyAnalysisIntent):
#     run_deployment(
#         "Fetch Study and Samples from ENA/ena_fetch_study_deployment",
#         timeout=0,
#         parameters={"accession": payload.study_accession},
#     )
#     return
