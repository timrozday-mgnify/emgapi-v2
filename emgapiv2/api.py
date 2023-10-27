from typing import List

from django.shortcuts import get_object_or_404
from ninja import NinjaAPI, ModelSchema, Schema
from prefect.deployments import run_deployment

import analyses.models
from workflows.ena_fetch_study_flow import ena_fetch_study_flow

api = NinjaAPI()


class MGnifyStudy(ModelSchema):
    class Config:
        model = analyses.models.Study
        model_fields = ['accession', 'ena_study']


class MGnifySample(ModelSchema):
    class Config:
        model = analyses.models.Sample
        model_fields = ['accession', 'ena_sample']


class MGnifyAnalysis(ModelSchema):
    class Config:
        model = analyses.models.Analysis
        model_fields = ['accession', 'study', 'results_dir']


@api.get("/studies/{accession}}", response=MGnifyStudy)
def get_mgnify_study(request, accession: str):
    study = get_object_or_404(analyses.models.Study, accession=accession)
    return study


@api.get("/studies", response=List[MGnifyStudy])
def list_mgnify_studies(request):
    qs = analyses.models.Study.objects.all()
    return qs


@api.get("/analyses/{accession}}", response=MGnifyAnalysis)
def get_mgnify_analysis(request, accession: str):
    analysis = get_object_or_404(analyses.models.Analysis, accession=accession)
    return analysis


@api.get("/analyses", response=List[MGnifyAnalysis])
def list_mgnify_analyses(request):
    qs = analyses.models.Analysis.objects.all()
    return qs


class StudyAnalysisIntent(Schema):
    study_accession: str


@api.post("/analyse")
def create_study_analysis_intent(request, payload: StudyAnalysisIntent):
    run_deployment(
        'Fetch Study and Samples from ENA/ena_fetch_study_deployment',
        timeout=0,
        parameters={
            'accession': payload.study_accession
        })
    return
