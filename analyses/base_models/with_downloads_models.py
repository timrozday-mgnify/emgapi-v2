from __future__ import annotations

from enum import Enum
from typing import Optional, Union, List

from django.db import models
from pydantic import BaseModel


class DownloadType(str, Enum):
    SEQUENCE_DATA = "Sequence data"
    FUNCTIONAL_ANALYSIS = "Functional analysis"
    TAXONOMIC_ANALYSIS = "Taxonomic analysis"
    STATISTICS = "Statistics"
    NON_CODING_RNAS = "non-coding RNAs"
    GENOME_ANALYSIS = "Genome analysis"
    RO_CRATE = "Analysis RO Crate"
    OTHER = "Other"


class DownloadFileType(str, Enum):
    FASTA = "fasta"
    TSV = "tsv"
    BIOM = "biom"
    CSV = "csv"
    JSON = "json"
    SVG = "svg"
    TREE = "tree"  # e.g. newick
    OTHER = "other"


class DownloadFile(BaseModel):
    """
    A download file schema for use in the `downloads` list.
    """

    path: str  # relative path from results dir of the object these downloads are for (e.g. the Analysis)
    alias: str  # an alias for the file, unique within the downloads list
    download_type: DownloadType
    file_type: DownloadFileType
    long_description: str
    short_description: str

    parent_identifier: Optional[
        Union[str, int]
    ] = None  # e.g. the accession of an Analysis this download is for


class WithDownloadsModel(models.Model):
    """
    Abstract model providing a `downloads` field which is a list of lightly schema'd downloadable files.

     e.g.

        sequence_file = DownloadFile(
            path='sequence_data/sequences.fasta',
            alias='err1.fasta',
            download_type=DownloadType.SEQUENCE_DATA,
            file_type=DownloadFileType.FASTA
        )

        my_model.add_download(sequence_file)
    """

    DOWNLOAD_PARENT_IDENTIFIER_ATTR: str = None

    downloads = models.JSONField(blank=True, default=list)

    def add_download(self, download: DownloadFile):
        if download.alias in [dl.get("alias") for dl in self.downloads]:
            raise Exception(
                f"Duplicate download alias found in {self}.downloads list - not adding {download.path}"
            )

        self.downloads.append(download.model_dump(exclude={"parent_identifier"}))
        self.save()

    @property
    def downloads_as_objects(self) -> List[DownloadFile]:
        return [
            DownloadFile.model_validate(
                dict(
                    **dl,
                    parent_identifier=getattr(
                        self, self.DOWNLOAD_PARENT_IDENTIFIER_ATTR
                    ),
                )
            )
            for dl in self.downloads
        ]

    class Meta:
        abstract = True
