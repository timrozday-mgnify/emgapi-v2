import re
from pathlib import Path

from prefect import task, get_run_logger
from prefect.tasks import task_input_hash

from activate_django_first import EMG_CONFIG

import analyses.models
from workflows.flows.analyse_study_tasks.analysis_states import AnalysisStates
from workflows.prefect_utils.analyses_models_helpers import task_mark_analysis_status


@task(
    cache_key_fn=task_input_hash,
)
def sanity_check_rawreads_results(
    current_outdir: Path, analysis: analyses.models.Analysis
):
    """
    qc-stats:
        required:
         - ${run_id}_fastp.json
    decontam-stats:
        required:
         - host
            - ${run_id}_(short|long)_read_host_all_summary_stats.txt
            - ${run_id}_(short|long)_read_host_mapped_summary_stats.txt
            - ${run_id}_(short|long)_read_host_unmapped_summary_stats.txt
        optional:
         - phix
            - ${run_id}_(short|long)_read_phix_all_summary_stats.txt
            - ${run_id}_(short|long)_read_phix_mapped_summary_stats.txt
            - ${run_id}_(short|long)_read_phix_unmapped_summary_stats.txt
    taxonomy-summary:
        required:
         - SILVA-LSU
            - krona
               - ${run_id}_SILVA-LSU.html
            - mapseq
               - ${run_id}_SILVA-LSU.mseq
            - ${run_id}_SILVA-LSU.txt
         - SILVA-SSU
            - krona
               - ${run_id}_SILVA-SSU.html
            - mapseq
               - ${run_id}_SILVA-SSU.mseq
            - ${run_id}_SILVA-SSU.txt
         - mOTUs
            - krona
               - ${run_id}_mOTUs.html
            - raw
               - ${run_id}_mOTUs.out
            - ${run_id}_mOTUs.txt
    function-summary:
        required:
         - Pfam-A
            - raw
               - ${run_id}_Pfam-A.domtbl
            - ${run_id}_Pfam-A.txt
    """
    logger = get_run_logger()
    reason = None
    run_id = analysis.run.first_accession
    qc_folder = Path(
        f"{current_outdir}/{EMG_CONFIG.rawreads_pipeline.qc_folder}"
    )
    decontam_folder = Path(
        f"{current_outdir}/{EMG_CONFIG.rawreads_pipeline.decontam_folder}"
    )
    taxonomy_summary_folder = Path(
        f"{current_outdir}/{EMG_CONFIG.rawreads_pipeline.taxonomy_summary_folder}"
    )
    functional_summary_folder = Path(
        f"{current_outdir}/{EMG_CONFIG.rawreads_pipeline.function_summary_folder}"
    )

    logger.info(f"Looking for {run_id} QC folder in {qc_folder}")
    logger.info(f"Looking for {run_id} Decontam folder in {decontam_folder}")
    logger.info(f"Looking for {run_id} Taxonomy summary folder in {taxonomy_summary_folder}")
    logger.info(f"Looking for {run_id} Functional summary folder in {functional_summary_folder}")

    # Decontam folder
    if decontam_folder.exists():
        host_folder = Path(f"{decontam_folder}/host")
        phix_folder = Path(f"{decontam_folder}/phix")

        if host_folder.exists():
            fps = [str(fp.name) for fp in host_folder.iterdir()]
            if not all([
                any([re.match(r'.*_all_summary_stats.txt$', fp) for fp in fps]),
                any([re.match(r'.*_mapped_summary_stats.txt$', fp) for fp in fps]),
                any([re.match(r'.*_unmapped_summary_stats.txt$', fp) for fp in fps]),
            ]):
                reason = f"Unexpected files in {EMG_CONFIG.rawreads_pipeline.decontam_folder} host folder"

        if phix_folder.exists():
            fps = [str(fp.name) for fp in phix_folder.iterdir()]
            if not all([
                any([re.match(r'.*_all_summary_stats.txt$', fp) for fp in fps]),
                any([re.match(r'.*_mapped_summary_stats.txt$', fp) for fp in fps]),
                any([re.match(r'.*_unmapped_summary_stats.txt$', fp) for fp in fps]),
            ]):
                reason = f"Unexpected files in {EMG_CONFIG.rawreads_pipeline.decontam_folder} phix folder"

    # FUNCTIONAL SUMMARY folder:
    if functional_summary_folder.exists():
        func_dbs = ["Pfam-A"]
        for db in functional_summary_folder.iterdir():
            if db.name in func_dbs:
                raw = Path(f"{db}/raw/{run_id}_{db.name}.domtbl")
                txt = Path(f"{db}/{run_id}_{db.name}.txt")
                if not (
                    raw.exists() and txt.exists()
                ):
                    reason = f"missing file in {db}"
            else:
                reason = f"unknown {db} in {EMG_CONFIG.rawreads_pipeline.function_summary_folder}"
    else:
        reason = f"No {EMG_CONFIG.rawreads_pipeline.function_summary_folder} folder"

    # TAXONOMY SUMMARY folder:
    if taxonomy_summary_folder.exists():
        tax_dbs = ["SILVA-SSU", "SILVA-LSU", "mOTUs"]
        for db in taxonomy_summary_folder.iterdir():
            if db.name in tax_dbs:
                html = Path(f"{db}/krona/{run_id}_{db.name}.html")
                if db.name in {"SILVA-SSU", "SILVA-LSU"}:
                    raw = Path(f"{db}/mapseq/{run_id}_{db.name}.mseq")
                else:
                    raw = Path(f"{db}/raw/{run_id}_{db.name}.out")
                txt = Path(f"{db}/{run_id}_{db.name}.txt")
                if not (
                    html.exists() and raw.exists() and txt.exists()
                ):
                    reason = f"missing file in {db}"
            else:
                reason = f"unknown {db} in {EMG_CONFIG.rawreads_pipeline.taxonomy_summary_folder}"
    else:
        reason = f"No {EMG_CONFIG.rawreads_pipeline.taxonomy_summary_folder} folder"

    # QC mandatory folder
    if qc_folder.exists():
        if not Path(f"{qc_folder}/fastp/{run_id}_fastp.json").exists():
            reason = f"No required fastp.json in {EMG_CONFIG.rawreads_pipeline.qc_folder} folder"
    else:
        reason = f"No {EMG_CONFIG.rawreads_pipeline.qc_folder} folder"

    logger.info(f"Post sanity check for {run_id}: {reason}")

    if reason:
        task_mark_analysis_status(
            analysis,
            status=AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED,
            reason=reason,
        )
