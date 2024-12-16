import logging
from pathlib import Path

import pandas as pd
from django.conf import settings

import analyses.models
from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
)
from workflows.data_io_utils.csv.csv_comment_handler import (
    CSVDelimiter,
    move_file_pointer_past_comment_lines,
)
from workflows.data_io_utils.file_rules.common_rules import (
    DirectoryExistsRule,
    FileExistsRule,
    FileIsNotEmptyRule,
)
from workflows.data_io_utils.file_rules.mgnify_v6_result_rules import (
    FileConformsToTaxonomyTSVSchemaRule,
    GlobOfTaxonomyFolderHasHtmlAndMseqRule,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File

EMG_CONFIG = settings.EMG_CONFIG

RESULT_FILESYSTEM_LABELS_FOR_TAXONOMY_SOURCES = {
    analyses.models.Analysis.TaxonomySources.SSU: "SILVA-SSU",
    analyses.models.Analysis.TaxonomySources.LSU: "SILVA-LSU",
    analyses.models.Analysis.TaxonomySources.PR2: "PR2",
    analyses.models.Analysis.TaxonomySources.UNITE: "UNITE",
    analyses.models.Analysis.TaxonomySources.ITS_ONE_DB: "ITSoneDB",
    analyses.models.Analysis.TaxonomySources.DADA2_PR2: "DADA2-PR2",
    analyses.models.Analysis.TaxonomySources.DADA2_SILVA: "DADA2-SILVA",
}


def import_taxonomy(
    analysis: analyses.models.Analysis,
    dir_for_analysis: Path,
    source: analyses.models.Analysis.TaxonomySources,
    allow_non_exist: bool = True,
):
    fs_label_for_tax_source = RESULT_FILESYSTEM_LABELS_FOR_TAXONOMY_SOURCES.get(source)

    if not fs_label_for_tax_source:
        raise NotImplementedError(
            f"There is no support for importing {source} annotations because a source directory is not known for those."
        )

    if source in [
        analyses.models.Analysis.TaxonomySources.DADA2_PR2,
        analyses.models.Analysis.TaxonomySources.DADA2_SILVA,
    ]:
        raise NotImplementedError(
            f"We have not yet added support for variable region file naming for DADA2 source {source}"
        )

    if not allow_non_exist:
        tax_dir = Directory(
            path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
            / EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder  # taxonomy-summary/
            / fs_label_for_tax_source,  # SILVA-SSU/
            rules=[DirectoryExistsRule],
            glob_rules=[GlobOfTaxonomyFolderHasHtmlAndMseqRule],
        )
    else:
        tax_dir = Directory(
            path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
            / EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder  # taxonomy-summary/
            / fs_label_for_tax_source,  # SILVA-SSU/
        )

    if not tax_dir.path.is_dir():
        print(f"No tax dir at {tax_dir.path} – no {source} taxa to import.")
        return

    tax_dir.files.append(
        File(
            path=tax_dir.path
            / f"{analysis.run.first_accession}_{fs_label_for_tax_source}.tsv",
            rules=[
                FileExistsRule,
                FileIsNotEmptyRule,
                FileConformsToTaxonomyTSVSchemaRule,
            ],
        )
    )

    with tax_dir.files[0].path.open("r") as tax_tsv:
        move_file_pointer_past_comment_lines(
            tax_tsv, delimiter=CSVDelimiter.TAB, comment_char="#"
        )
        try:
            tax_df = pd.read_csv(tax_tsv, sep=CSVDelimiter.TAB)
        except pd.errors.EmptyDataError:
            logging.error(
                f"Found empty taxonomy TSV in {tax_dir}. Probably unit testing, otherwise we should never be here"
            )
            return

    tax_df = tax_df.rename(columns={"taxonomy": "organism"})
    tax_df["count"] = None
    tax_df: pd.DataFrame = tax_df[["organism", "count"]]

    taxonomies = analysis.annotations.get(analysis.TAXONOMIES, {})
    if not taxonomies:
        taxonomies = {}
    taxonomies[analysis.TaxonomySources.SSU.value] = tax_df.to_dict(orient="records")
    analysis.annotations[analysis.TAXONOMIES] = taxonomies

    tax_dir.files.append(
        File(
            path=tax_dir.path / f"{analysis.run.first_accession}.html",
            rules=[
                FileExistsRule,
                FileIsNotEmptyRule,
            ],
        )
    )
    analysis.add_download(
        DownloadFile(
            path=tax_dir.files[-1].path.relative_to(analysis.results_dir),
            file_type=DownloadFileType.HTML,
            alias=f"{analysis.run.first_accession}_{source.value}.html",
            download_type=DownloadType.TAXONOMIC_ANALYSIS,
            parent_identifier=analysis.accession,
            short_description=f"{source.value} Krona plot",
            long_description=f"Krona plot webpage showing taxonomic assignments from {source.value} annotation",
        )
    )
    analysis.save()
