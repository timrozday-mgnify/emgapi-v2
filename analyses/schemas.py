from __future__ import annotations

from enum import Enum
from typing import List, Optional, Union

from ninja import Field, ModelSchema, Schema
from typing_extensions import Annotated

import analyses.models
from analyses.base_models.with_downloads_models import DownloadFile
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


class MGnifyAnalysisDownloadFile(Schema, DownloadFile):
    path: Annotated[str, Field(exclude=True)]
    parent_identifier: Annotated[Union[int, str], Field(exclude=True)]

    url: str = None

    @staticmethod
    def resolve_url(obj: MGnifyAnalysisDownloadFile):
        return EMG_CONFIG.legacy_service.emg_analysis_download_url_pattern.format(
            id=obj.parent_identifier, alias=obj.alias
        )


class MGnifyAnalysis(ModelSchema):
    study_accession: str

    class Meta:
        model = analyses.models.Analysis
        fields = ["accession"]


class AnalysedRun(ModelSchema):
    accession: str = Field(..., alias="first_accession", examples=["ERR0000001"])
    instrument_model: str = Field(..., examples=["Illumina HiSeq 2000"])
    instrument_platform: str = Field(..., examples=["Illumina"])

    class Meta:
        model = analyses.models.Run
        fields = ["instrument_model", "instrument_platform"]


class MGnifyAnalysisDetail(MGnifyAnalysis):
    downloads: List[MGnifyAnalysisDownloadFile] = Field(
        ..., alias="downloads_as_objects"
    )
    run_accession: Optional[str] = Field(
        ...,
        description="Accession number of the run this analysis is of, if this is a raw read analysis.",
        examples=["ERR0000001"],
    )
    sample_accession: Optional[str] = Field(..., examples=["ERS0000001"])
    assembly_accession: Optional[str] = Field(
        ...,
        description="Accession number of the assembly this analysis is of, if this is an assembly analysis.",
        examples=["ERZ0000001"],
    )
    experiment_type: Optional[analyses.models.Run.ExperimentTypes]
    pipeline_version: Optional[analyses.models.Analysis.PipelineVersions]
    read_run: Optional[AnalysedRun] = Field(
        ...,
        alias="raw_run",
        description="Metadata associated with the original read run this analysis is based on, whether or not those reads were assembled.",
    )

    class Meta:
        model = analyses.models.Analysis
        fields = [
            "accession",
            "sample_accession",
            "assembly_accession",
            "experiment_type",
            "pipeline_version",
        ]


class MGnifyAnalysisTypedAnnotation(Schema):
    count: int
    description: Optional[str] = None  # for functional
    organism: Optional[str] = None  # for taxonomic


class MGnifyAnalysisWithAnnotations(MGnifyAnalysisDetail):
    annotations: dict[
        str,
        Union[
            List[MGnifyAnalysisTypedAnnotation],
            dict[str, Optional[List[MGnifyAnalysisTypedAnnotation]]],
        ],
    ] = Field(
        ...,
        examples=[
            {
                "pfams": [
                    {
                        "count": 1,
                        "description": "PFAM1",
                        "organism": None,
                    }
                ],
                "taxonomics": {
                    "lsu": [
                        {
                            "count": 1,
                            "description": None,
                            "organism": "Bacteria",
                        }
                    ]
                },
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
