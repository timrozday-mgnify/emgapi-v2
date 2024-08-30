from __future__ import annotations

from enum import Enum
from typing import Optional, List, Union

from ninja import Schema, ModelSchema, Field
from typing_extensions import Annotated

import analyses.models
from analyses.models import Analysis
from emgapiv2.settings import EMG_CONFIG


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


class MGnifyAnalysisDownloadFile(Schema, Analysis.DownloadFile):
    path: Annotated[str, Field(exclude=True)]
    parent_identifier: Annotated[Union[int, str], Field(exclude=True)]

    url: str = None

    @staticmethod
    def resolve_url(obj: MGnifyAnalysisDownloadFile):
        return EMG_CONFIG.legacy_service.emg_analysis_download_url_pattern.format(
            id=obj.parent_identifier, alias=obj.alias
        )


class MGnifyAnalysis(ModelSchema):
    study_accession: str = Field(..., alias="study_id")

    class Meta:
        model = analyses.models.Analysis
        fields = ["accession"]


class MGnifyAnalysisDetail(ModelSchema):
    study_accession: str = Field(..., alias="study_id")
    downloads: List[MGnifyAnalysisDownloadFile] = Field(
        ..., alias="downloads_as_objects"
    )

    class Meta:
        model = analyses.models.Analysis
        fields = ["accession"]


class MGnifyAnalysisTypedAnnotation(Schema):
    count: int
    description: Optional[str] = None  # for functional
    organism: Optional[str] = None  # for taxonomic


class MGnifyAnalysisWithAnnotations(MGnifyAnalysisDetail):
    annotations: dict[str, List[MGnifyAnalysisTypedAnnotation]] = Field(
        ...,
        examples=[
            {
                "pfams": [
                    {
                        "count": 1,
                        "description": "PFAM1",
                        "organism": None,
                    }
                ]
            }
        ],
    )

    class Meta:
        model = analyses.models.Analysis
        fields = ["accession", "annotations"]


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
    taxonomies_ssu: str = analyses.models.Analysis.TAXONOMIES_SSU
    taxonomies_lsu: str = analyses.models.Analysis.TAXONOMIES_LSU
    taxonomies_itsonedb: str = analyses.models.Analysis.TAXONOMIES_ITS_ONE_DB
    taxonomies_unite: str = analyses.models.Analysis.TAXONOMIES_UNITE
    taxonomies_pr2: str = analyses.models.Analysis.TAXONOMIES_PR2
    antismash_gene_clusters: str = analyses.models.Analysis.ANTISMASH_GENE_CLUSTERS
    pfams: str = analyses.models.Analysis.PFAMS


class StudyAnalysisIntent(Schema):
    study_accession: str


# AnalysisSchema = create_model()


# class MGnifyAnalysisWithTypedAnnotations(Schema):
#     accession: str
#     annotations_list = List[MGnifyAnalysisTypedAnnotation]
