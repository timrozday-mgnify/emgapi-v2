import json
import logging
from pathlib import Path
from typing import List, Literal, Optional

import pandas as pd
from django.conf import settings
from pydantic import BaseModel, Field

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
    GlobHasFilesCountRule,
)
from workflows.data_io_utils.file_rules.mgnify_v6_result_rules import (
    FileConformsToTaxonomyTSVSchemaRule,
    FileConformsToFunctionalTSVSchemaRule,
    GlobOfQcFolderHasFastpAndMultiqc,
    GlobOfTaxonomyFolderHasHtmlAndMseqRule,
    GlobOfAsvFolderHasRegionFolders,
)
from workflows.data_io_utils.file_rules.nodes import Directory, File

EMG_CONFIG = settings.EMG_CONFIG

_TAXONOMY = analyses.models.Analysis.TAXONOMIES
_FUNCTIONAL = analyses.models.Analysis.FUNCTIONAL


class RawReadsV6TaxonomyFolderSchema(BaseModel):
    taxonomy_summary_folder_name: Path
    expect_krona: bool = Field(True)
    expect_mseq: bool = Field(True)
    expect_tsv: bool = Field(True)
    expect_raw: bool = Field(False)


class RawReadsV6FunctionFolderSchema(BaseModel):
    function_summary_folder_name: Path
    expect_raw: bool = Field(False)


RESULT_SCHEMAS_FOR_TAXONOMY_SOURCES = {
    analyses.models.Analysis.TaxonomySources.MOTUS: RawReadsV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("mOTUs"),
        expect_mseq = False,
    ),
    analyses.models.Analysis.TaxonomySources.SSU: RawReadsV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("SILVA-SSU"),
        expect_raw = False,
    ),
    analyses.models.Analysis.TaxonomySources.LSU: RawReadsV6TaxonomyFolderSchema(
        taxonomy_summary_folder_name=Path("SILVA-LSU"),
        expect_raw = False,
    ),
}


RESULT_SCHEMAS_FOR_FUNCTIONAL_SOURCES = {
    analyses.models.Analysis.FunctionalSources.PFAM: RawReadsV6FunctionFolderSchema(
        function_summary_folder_name=Path("Pfam-A"),
    ),
}


def get_annotations_from_tax_table(tax_table: File) -> (List[dict], Optional[int]):
    """
    Reads the taxonomy TSV table from tax_table.path

    :param tax_table: File whose property .path points at a TSV file
    :return:  A records-oriented list of taxonomies (lineages) present along with their read count, and the total read count.
    """
    with tax_table.path.open("r") as tax_tsv:
        move_file_pointer_past_comment_lines(
            tax_tsv, delimiter=CSVDelimiter.TAB, comment_char="#"
        )
        try:
            tax_df = pd.read_csv(tax_tsv, sep=CSVDelimiter.TAB, usecols=[0, 1, 2])
        except pd.errors.EmptyDataError:
            logging.error(
                f"Found empty taxonomy TSV at {tax_table.path}. Probably unit testing, otherwise we should never be here"
            )
            return [], 0

    for read_count_column_possible_name in ["SSU", "mOTUs", "LSU"]:
        if read_count_column_possible_name in tax_df.columns:
            tax_df.rename(
                columns={read_count_column_possible_name: "count"}, inplace=True
            )
            break
    else:
        tax_df["count"] = 0

    tax_df["count"] = (
        pd.to_numeric(tax_df["count"], errors="coerce").fillna(1).astype(int)
    )

    tax_df = tax_df.rename(columns={"taxonomy": "organism"})
    tax_df: pd.DataFrame = tax_df[["organism", "count"]]

    return tax_df.to_dict(orient="records"), int(tax_df["count"].sum())


def import_taxonomy(
    analysis: analyses.models.Analysis,
    dir_for_analysis: Path,
    source: analyses.models.Analysis.TaxonomySources,
    allow_non_exist: bool = True,
):
    schema = RESULT_SCHEMAS_FOR_TAXONOMY_SOURCES.get(source)

    if not schema:
        raise NotImplementedError(
            f"There is no support for importing {source} annotations because a directory structure is not known for those."
        )

    # FILE CHECKS
    # TODO: dedupe this with sanity check flow
    dir_rules = [DirectoryExistsRule] if not allow_non_exist else []
    glob_rules = []
    if schema.expect_mseq and schema.expect_krona and not allow_non_exist:
        glob_rules.append(GlobOfTaxonomyFolderHasHtmlAndMseqRule)

    tax_dir = Directory(
        path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
        / EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder  # taxonomy-summary/
        / schema.taxonomy_summary_folder_name,  # SILVA-SSU/
        rules=dir_rules,
        glob_rules=glob_rules,
    )

    if not tax_dir.path.is_dir():
        print(f"No tax dir at {tax_dir.path} – no {source} taxa to import.")
        return

    if schema.expect_tsv:
        tax_table = File(
            path=tax_dir.path
            / f"{analysis.run.first_accession}_{schema.taxonomy_summary_folder_name}.txt",
            rules=[
                FileExistsRule,
                FileIsNotEmptyRule,
                FileConformsToTaxonomyTSVSchemaRule,
            ],
        )

        tax_dir.files.append(tax_table)

    if schema.expect_krona:
        krona_files = list(tax_dir.path.glob(f"krona/{analysis.run.first_accession}_{schema.taxonomy_summary_folder_name}.html"))
        for krona_file in krona_files:
            krona = File(
                path=Path(krona_file),
                rules=[
                    FileExistsRule,
                    FileIsNotEmptyRule,
                ],
            )
            tax_dir.files.append(krona)

    if schema.expect_mseq:
        mapseq = File(
            path=tax_dir.path
            / f"mapseq/{analysis.run.first_accession}_{schema.taxonomy_summary_folder_name}.mseq",
            rules=[
                FileExistsRule,
            ],
        )
        tax_dir.files.append(mapseq)

    if schema.expect_raw:
        raw_out = File(
            path=tax_dir.path
            / f"raw/{analysis.run.first_accession}_{schema.taxonomy_summary_folder_name}.out",
            rules=[
                FileExistsRule,
            ],
        )
        tax_dir.files.append(raw_out)

    # DOWNLOAD FILE IMPORTS
    if schema.expect_tsv:
        taxonomies_to_import, total_read_count = get_annotations_from_tax_table(
            tax_table
        )
        analysis_taxonomies = analysis.annotations.get(analysis.TAXONOMIES, {})
        if not analysis_taxonomies:
            analysis_taxonomies = {}
        analysis_taxonomies[source] = taxonomies_to_import
        analysis.annotations[analysis.TAXONOMIES] = analysis_taxonomies

        analysis.add_download(
            DownloadFile(
                path=tax_table.path.relative_to(analysis.results_dir),
                file_type=DownloadFileType.TSV,
                alias=tax_dir.path.name,
                download_type=DownloadType.TAXONOMIC_ANALYSIS,
                download_group=f"{_TAXONOMY}.{schema.reference_type}.{schema.taxonomy_summary_folder_name}",
                parent_identifier=analysis.accession,
                short_description=f"{schema.taxonomy_summary_folder_name} taxonomy table",
                long_description="Table with read counts for each taxonomic assignment",
            )
        )

    if schema.expect_krona:
        for krona in krona_files:
            analysis.add_download(
                DownloadFile(
                    path=Path(krona).relative_to(analysis.results_dir),
                    file_type=DownloadFileType.HTML,
                    alias=f"{Path(krona).stem}_{schema.taxonomy_summary_folder_name}{Path(krona).suffix}",
                    download_type=DownloadType.TAXONOMIC_ANALYSIS,
                    download_group=f"{_TAXONOMY}.{schema.reference_type}.{schema.taxonomy_summary_folder_name}",
                    parent_identifier=analysis.accession,
                    short_description=f"{schema.taxonomy_summary_folder_name} Krona plot",
                    long_description=f"Krona plot webpage showing taxonomic assignments from {schema.taxonomy_summary_folder_name} annotation",
                )
            )

    if schema.expect_mseq:
        analysis.add_download(
            DownloadFile(
                path=mapseq.path.relative_to(analysis.results_dir),
                file_type=DownloadFileType.TSV,
                alias=mapseq.path.name,
                download_type=DownloadType.TAXONOMIC_ANALYSIS,
                download_group=f"{_TAXONOMY}.{schema.reference_type}.{schema.taxonomy_summary_folder_name}",
                parent_identifier=analysis.accession,
                short_description=f"{schema.taxonomy_summary_folder_name} MAPseq output",
                long_description="MAPseq output table with taxonomic database hit details",
            )
        )

    if schema.expect_raw:
        analysis.add_download(
            DownloadFile(
                path=raw_out.path.relative_to(analysis.results_dir),
                file_type=DownloadFileType.TSV,
                alias=raw_out.path.name,
                download_type=DownloadType.TAXONOMIC_ANALYSIS,
                download_group=f"{_TAXONOMY}.{schema.reference_type}.{schema.taxonomy_summary_folder_name}",
                parent_identifier=analysis.accession,
                short_description=f"{schema.taxonomy_summary_folder_name} raw mOTUs output",
                long_description="mOTUs output table with taxonomic database hit details",
            )
        )

    analysis.save()


def get_annotations_from_func_table(func_table: File) -> (List[dict], Optional[int]):
    """
    Reads the functional profile TSV table from func_table.path

    :param func_table: File whose property .path points at a TSV file
    :return:  A records-oriented list of functions (genes) present along with their read count, and the total read count.
    """
    with func_table.path.open("r") as func_tsv:
        move_file_pointer_past_comment_lines(
            func_tsv, delimiter=CSVDelimiter.TAB, comment_char="#"
        )
        try:
            func_df = pd.read_csv(func_tsv, sep=CSVDelimiter.TAB, usecols=[0, 1, 2, 3], columns=['function', 'count', 'coverage_depth', 'coverage_breadth'])
        except pd.errors.EmptyDataError:
            logging.error(
                f"Found empty functional profile TSV at {func_table.path}. Probably unit testing, otherwise we should never be here"
            )
            return [], 0

    func_df["count"] = (
        pd.to_numeric(func_df["count"], errors="coerce").fillna(1).astype(int)
    )
    func_df["coverage_depth"] = (
        pd.to_numeric(func_df["coverage_depth"], errors="coerce").fillna(0).astype(float)
    )
    func_df["coverage_breadth"] = (
        pd.to_numeric(func_df["coverage_breadth"], errors="coerce").fillna(0).astype(float)
    )

    func_df: pd.DataFrame = func_df[["function", "count"]]

    return func_df.to_dict(orient="records"), int(func_df["count"].sum())


def import_functional(
    analysis: analyses.models.Analysis,
    dir_for_analysis: Path,
    source: analyses.models.Analysis.FunctionalSources,
    allow_non_exist: bool = True,
):
    schema = RESULT_SCHEMAS_FOR_FUNCTIONAL_SOURCES.get(source)

    if not schema:
        raise NotImplementedError(
            f"There is no support for importing {source} annotations because a directory structure is not known for those."
        )

    # FILE CHECKS
    # TODO: dedupe this with sanity check flow
    dir_rules = [DirectoryExistsRule] if not allow_non_exist else []

    func_dir = Directory(
        path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
        / EMG_CONFIG.rawreads_pipeline.function_summary_folder  # function-summary/
        / schema.function_summary_folder_name,  # Pfam-A/
        rules=dir_rules,
    )

    if not func_dir.path.is_dir():
        print(f"No func dir at {func_dir.path} – no {source} functions to import.")
        return

    if schema.expect_tsv:
        func_table = File(
            path=func_dir.path
            / f"{analysis.run.first_accession}_{schema.functional_profile_folder_name}.txt",
            rules=[
                FileExistsRule,
                FileIsNotEmptyRule,
                FileConformsToFunctionalTSVSchemaRule,
            ],
        )

        func_dir.files.append(func_table)

    if schema.expect_raw:
        raw_out = File(
            path=func_dir.path
            / f"raw/{analysis.run.first_accession}_{schema.functional_profile_folder_name}.out",
            rules=[
                FileExistsRule,
            ],
        )

        func_dir.files.append(raw_out)

    # DOWNLOAD FILE IMPORTS
    if schema.expect_tsv:
        functions_to_import, total_read_count = get_annotations_from_func_table(
            func_table
        )
        analysis_functions = analysis.annotations.get(analysis.FUNCTIONAL, {})
        if not analysis_functions:
            analysis_functions = {}
        analysis_functions[source] = functions_to_import
        analysis.annotations[analysis.FUNCTIONAL] = analysis_functions

        analysis.add_download(
            DownloadFile(
                path=func_table.path.relative_to(analysis.results_dir),
                file_type=DownloadFileType.TSV,
                alias=func_dir.path.name,
                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                download_group=f"{_FUNCTIONAL}.{schema.reference_type}.{schema.functional_summary_folder_name}",
                parent_identifier=analysis.accession,
                short_description=f"{schema.functional_summary_folder_name} functional profile table",
                long_description="Table with read counts for each functional assignment",
            )
        )

    if schema.expect_raw:
        analysis.add_download(
            DownloadFile(
                path=raw_out.path.relative_to(analysis.results_dir),
                file_type=DownloadFileType.TSV,
                alias=raw_out.path.name,
                download_type=DownloadType.FUNCTIONAL_ANALYSIS,
                download_group=f"{_FUNCTIONAL}.{schema.reference_type}.{schema.functional_summary_folder_name}",
                parent_identifier=analysis.accession,
                short_description=f"{schema.functional_summary_folder_name} raw HMMer output",
                long_description="HMMer output table with hit details",
            )
        )

    analysis.save()


def import_qc(
    analysis: analyses.models.Analysis,
    dir_for_analysis: Path,
    allow_non_exist: bool = True,
):
    if not allow_non_exist:
        qc_dir = Directory(
            path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
            / EMG_CONFIG.amplicon_pipeline.qc_folder,  # qc/
            rules=[DirectoryExistsRule],
            glob_rules=[GlobOfQcFolderHasFastpAndMultiqc],
        )
    else:
        qc_dir = Directory(
            path=dir_for_analysis  # /hps/prod/...../abc123/SRR999/
            / EMG_CONFIG.amplicon_pipeline.qc_folder  # qc/
        )

    if not qc_dir.path.is_dir():
        print(f"No qc dir at {qc_dir.path}. Nothing to import.")
        return

    multiqc = File(
        path=qc_dir.path / f"{analysis.run.first_accession}_multiqc_report.html",
        rules=[
            FileExistsRule,
        ],
    )
    qc_dir.files.append(multiqc)
    analysis.add_download(
        DownloadFile(
            path=multiqc.path.relative_to(analysis.results_dir),
            file_type=DownloadFileType.HTML,
            alias=multiqc.path.name,
            download_type=DownloadType.QUALITY_CONTROL,
            download_group="quality_control",
            parent_identifier=analysis.accession,
            short_description="MultiQC quality control report",
            long_description="MultiQC webpage showing quality control steps and metrics",
        )
    )
    analysis.save()

    fastp = File(
        path=qc_dir.path / f"{analysis.run.first_accession}.fastp.json",
        rules=(
            [
                FileExistsRule,
                FileIsNotEmptyRule,
            ]
            if not allow_non_exist
            else []
        ),
    )
    if not fastp.path.is_file():
        print(f"No fastp file for {analysis.run.first_accession}.")
        return

    qc_dir.files.append(fastp)

    with fastp.path.open("r") as fastp_reader:
        fastp_content = json.load(fastp_reader)
    fastp_summary = fastp_content.get("summary")

    if not fastp_summary:
        print(f"No fastp summary for {analysis.run.first_accession}.")
        return

    # TODO: a pydantic schema for this file would be nice... but will be very different for other pipelines
    analysis.quality_control = fastp_summary
    analysis.save()
