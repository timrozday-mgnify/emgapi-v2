from typing import List

from django.shortcuts import get_object_or_404
from ninja import NinjaAPI, ModelSchema, Schema

import analyses.models

api = NinjaAPI(
    title="MGnify API",
    description="The API for [MGnify](https://www.ebi.ac.uk/metagenomics), "
    "EBI’s platform for the submission, analysis, discovery and comparison of metagenomic-derived datasets.",
    urls_namespace="api",
    csrf=True,
    version="2.0-alpha",
)


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


class MGnifyAssemblyAnalysisRequestCreate(ModelSchema):
    class Config:
        model = analyses.models.AssemblyAnalysisRequest
        model_fields = ["requestor", "request_metadata"]


class MGnifyAssemblyAnalysisRequest(ModelSchema):
    class Config:
        model = analyses.models.AssemblyAnalysisRequest
        model_fields = ["requestor", "status", "study", "request_metadata", "id"]


@api.get("/studies/{accession}", response=MGnifyStudy)
def get_mgnify_study(request, accession: str):
    study = get_object_or_404(analyses.models.Study, accession=accession)
    return study


@api.get("/studies", response=List[MGnifyStudy])
def list_mgnify_studies(request):
    qs = analyses.models.Study.objects.all()
    return qs


@api.get("/analyses/{accession}", response=MGnifyAnalysis)
def get_mgnify_analysis(request, accession: str):
    analysis = get_object_or_404(analyses.models.Analysis, accession=accession)
    return analysis


@api.get("/analyses", response=List[MGnifyAnalysis])
def list_mgnify_analyses(request):
    qs = analyses.models.Analysis.objects.all()
    return qs


class StudyAnalysisIntent(Schema):
    study_accession: str


@api.get("/analysis_requests", response=List[MGnifyAssemblyAnalysisRequest])
def list_assembly_analysis_requests(request):
    qs = analyses.models.AssemblyAnalysisRequest.objects.all()
    # TODO: perms
    return qs


@api.get("/analysis_requests/{id}", response=MGnifyAssemblyAnalysisRequest)
def get_assembly_analysis_requests(request, analysis_request_id: int):
    return analyses.models.AssemblyAnalysisRequest.objects.get(id=analysis_request_id)
    # TODO: perms


@api.post("/analysis_requests", response=MGnifyAssemblyAnalysisRequest)
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
