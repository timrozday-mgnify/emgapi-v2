import gzip
import json
import logging
import os
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import List, Optional
from unittest.mock import patch

import pytest
from django.conf import settings
from prefect.artifacts import Artifact
from pydantic import BaseModel

import analyses.models
from workflows.data_io_utils.file_rules.base_rules import FileRule
from workflows.data_io_utils.file_rules.common_rules import GlobHasFilesCountRule
from workflows.data_io_utils.file_rules.nodes import Directory
from workflows.ena_utils.ena_api_requests import ENALibraryStrategyPolicy
from workflows.flows.analysis_rawreads_study import analysis_rawreads_study
from workflows.prefect_utils.analyses_models_helpers import get_users_as_choices
from workflows.prefect_utils.testing_utils import (
    should_not_mock_httpx_requests_to_prefect_server,
)

EMG_CONFIG = settings.EMG_CONFIG


def generate_fake_rawreads_pipeline_results(results_dir, sample_accession):
    """
    Generate fake raw-reads pipeline results for testing.

    Based on the directory structure provided in the issue description.

    :param results_dir: Directory to create the fake results in
    :param sample_accession: Sample accession to use in file names
    """
    # Create the main directory
    os.makedirs(results_dir, exist_ok=True)

    # Create function-summary directory and subdirectories
    func_dir = f"{results_dir}/{sample_accession}/function-summary"
    pfam_dir = f"{func_dir}/Pfam-A"
    os.makedirs(pfam_dir, exist_ok=True)
    with open(f"{pfam_dir}/{sample_accession}_Pfam-A.txt", "wt") as f:
        f.write(
            """\
            # Query\tAccession\tRead Count\tCoverage Depth\tCoverage Breadth
            PF02826.25\t43\t9.162921348314606\t0.9606741573033708
            PF00389.36\t14\t3.8358208955223883\t0.6567164179104478
            PF10417.14\t5\t3.292682926829268\t0.7317073170731707
            PF13614.12\t15\t3.0338983050847457\t0.807909604519774
            PF03061.28\t7\t3.0126582278481013\t0.9620253164556962
            PF00004.35\t12\t3.0\t0.7175572519083969
            PF13173.12\t10\t2.7829457364341086\t0.8914728682170543
            PF07724.20\t12\t2.63855421686747\t0.7650602409638554
            PF00198.28\t13\t2.189655172413793\t0.8836206896551724
            PF02737.24\t11\t2.1666666666666665\t0.6833333333333333
            PF00393.24\t15\t1.8724137931034484\t0.8241379310344827
            PF07726.17\t6\t1.6717557251908397\t0.6564885496183206
            PF01367.25\t4\t1.597938144329897\t0.5257731958762887
            PF17862.7\t2\t1.511111111111111\t0.8222222222222222
            PF13555.12\t3\t1.1639344262295082\t0.5573770491803278
            PF02739.22\t4\t1.146341463414634\t0.42073170731707316
            PF20789.3\t4\t1.1285714285714286\t0.39285714285714285
            PF13476.12\t6\t0.9154228855721394\t0.22885572139303484
            PF09820.15\t7\t0.8960573476702509\t0.5806451612903226
            PF02096.25\t3\t0.8220858895705522\t0.4171779141104294
            PF02872.23\t3\t0.6075949367088608\t0.43670886075949367
            PF13304.12\t5\t0.5723684210526315\t0.2894736842105263
            PF04463.17\t2\t0.5531914893617021\t0.3546099290780142
            PF06725.17\t1\t0.4444444444444444\t0.4444444444444444
            PF16193.11\t1\t0.43209876543209874\t0.43209876543209874
            PF06750.18\t1\t0.42857142857142855\t0.42857142857142855
            PF06439.18\t2\t0.39037433155080214\t0.37433155080213903
            PF09821.14\t1\t0.31932773109243695\t0.31932773109243695
            PF13245.12\t1\t0.2835820895522388\t0.2835820895522388
            PF12846.13\t2\t0.22099447513812154\t0.22099447513812154
            PF13604.12\t1\t0.19895287958115182\t0.19895287958115182
            """
        )
    os.makedirs(f"{pfam_dir}/raw", exist_ok=True)
    with open(f"{pfam_dir}/raw/{sample_accession}_Pfam-A.domtbl", "w"):
        pass


    # Create function-summary directory and subdirectories
    tax_dir = f"{results_dir}/{sample_accession}/taxonomic-summary"
    
    # mOTUs
    motus_dir = f"{tax_dir}/mOTUs"
    os.makedirs(motus_dir, exist_ok=True)
    with open(f"{motus_dir}/{sample_accession}_mOTUs.txt", "wt") as f:
        f.write(
            """\
            1.0\tk__Bacteria\tp__Firmicutes\tc__Bacilli\to__Lactobacillales\tf__Lactobacillaceae\tg__Lactobacillus\ts__Lactobacillus\tgasseri
            2.0\tk__Bacteria\tp__Actinobacteria\tc__Actinobacteria\to__Bifidobacteriales\tf__Bifidobacteriaceae\tg__Bifidobacterium\ts__Bifidobacterium\tlongum\t[Bifidobacterium\tlongum\tCAG:69/Bifidobacterium\tlongum]
            1.0\tk__Bacteria\tp__Bacteroidetes\tc__Bacteroidia\to__Bacteroidales\tf__Bacteroidaceae\tg__Bacteroides\ts__Bacteroides\tthetaiotaomicron
            """
        )
    os.makedirs(f"{motus_dir}/raw", exist_ok=True)
    with open(f"{motus_dir}/raw/{sample_accession}_mOTUs.out", "w"):
        pass
    os.makedirs(f"{motus_dir}/krona", exist_ok=True)
    with open(f"{motus_dir}/krona/{sample_accession}_mOTUs.html", "w"):
        pass

    # SILVA-SSU
    silvassu_dir = f"{tax_dir}/SILVA-SSU"
    os.makedirs(silvassu_dir, exist_ok=True)
    with open(f"{silvassu_dir}/{sample_accession}_SILVA-SSU.txt", "wt") as f:
        f.write(
            """\
            1\tsk__Bacteria\tk__\tp__Actinobacteria\tc__Actinobacteria\to__Bifidobacteriales\tf__Bifidobacteriaceae\tg__Bifidobacterium\ts__Bifidobacterium_breve
            3\tsk__Bacteria\tk__\tp__Actinobacteria\tc__Actinobacteria\to__Bifidobacteriales\tf__Bifidobacteriaceae\tg__Bifidobacterium\ts__Bifidobacterium_longum
            """
        )
    os.makedirs(f"{silvassu_dir}/mapseq", exist_ok=True)
    with open(f"{silvassu_dir}/mapseq/{sample_accession}_SILVA-SSU.mseq", "w"):
        pass
    os.makedirs(f"{silvassu_dir}/krona", exist_ok=True)
    with open(f"{silvassu_dir}/krona/{sample_accession}_SILVA-SSU.html", "w"):
        pass

    # SILVA-LSU
    silvalsu_dir = f"{tax_dir}/SILVA-LSU"
    os.makedirs(silvalsu_dir, exist_ok=True)
    with open(f"{silvalsu_dir}/{sample_accession}_SILVA-LSU.txt", "wt") as f:
        f.write(
            """\
            4\tsk__Bacteria\tk__\tp__Actinobacteria\tc__Actinobacteria\to__Bifidobacteriales\tf__Bifidobacteriaceae\tg__Bifidobacterium\ts__Bifidobacterium_breve
            18\tsk__Bacteria\tk__\tp__Actinobacteria\tc__Actinobacteria\to__Bifidobacteriales\tf__Bifidobacteriaceae\tg__Bifidobacterium\ts__Bifidobacterium_longum
            """
        )
    os.makedirs(f"{silvalsu_dir}/mapseq", exist_ok=True)
    with open(f"{silvalsu_dir}/mapseq/{sample_accession}_SILVA-LSU.mseq", "w"):
        pass
    os.makedirs(f"{silvalsu_dir}/krona", exist_ok=True)
    with open(f"{silvalsu_dir}/krona/{sample_accession}_SILVA-LSU.html", "w"):
        pass


    # Create qc directory and subdirectories
    qc_dir = f"{results_dir}/qc-stats"
    fastp_dir = f"{qc_dir}/fastp"
    os.makedirs(fastp_dir, exist_ok=True)
    with open(f"{fastp_dir}/{sample_accession}_fastp.json", "w"):
        pass

    # Create decontam directory and subdirectories
    decontam_dir = f"{results_dir}/decontam-stats"

    host_dir = f"{decontam_dir}/host"
    os.makedirs(host_dir, exist_ok=True)
    with open(f"{host_dir}/{sample_accession}_short_read_host_all_summary_stats.txt", "w"):
        pass
    with open(f"{host_dir}/{sample_accession}_short_read_host_mapped_summary_stats.txt", "w"):
        pass
    with open(f"{host_dir}/{sample_accession}_short_read_host_unmapped_summary_stats.txt", "w"):
        pass

    phix_dir = f"{decontam_dir}/phix"
    os.makedirs(phix_dir, exist_ok=True)
    with open(f"{phix_dir}/{sample_accession}_short_read_phix_all_summary_stats.txt", "w"):
        pass
    with open(f"{phix_dir}/{sample_accession}_short_read_phix_mapped_summary_stats.txt", "w"):
        pass
    with open(f"{phix_dir}/{sample_accession}_short_read_phix_unmapped_summary_stats.txt", "w"):
        pass


MockFileIsNotEmptyRule = FileRule(
    rule_name="File should not be empty (unit test mock)",
    test=lambda f: True,  # allows empty files created by mocks
)


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
@patch("workflows.flows.analyse_study_tasks.make_samplesheet_rawreads.queryset_hash")
@patch(
    "workflows.data_io_utils.mgnify_v6_utils.rawreads.FileIsNotEmptyRule",
    MockFileIsNotEmptyRule,
)
@pytest.mark.parametrize(
    "mock_suspend_flow_run", ["workflows.flows.analysis_rawreads_study"], indirect=True
)
def test_prefect_analyse_rawreads_flow(
    mock_queryset_hash_for_rawreads,
    prefect_harness,
    httpx_mock,
    mock_cluster_can_accept_jobs_yes,
    mock_start_cluster_job,
    mock_check_cluster_job_all_completed,
    raw_read_ena_study,
    mock_suspend_flow_run,
    admin_user,
    top_level_biomes,
):
    """
    Test should create/get ENA and MGnify study into DB.
    Create analysis for assembly runs and launch it with samplesheet.
    One assembly has all results, one assembly failed
    """
    mock_queryset_hash_for_rawreads.return_value = "abc123"

    study_accession = "PRJEB51728"
    all_results = ["ERR10889188", "ERR10889197", "ERR10889214", "ERR10889221"]

    # mock ENA response
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study"
        f"&query=%22%28study_accession%3D{study_accession}+OR+secondary_study_accession%3D{study_accession}%29%22"
        f"&fields=study_title%2Csecondary_study_accession"
        f"&limit=10"
        f"&format=json"
        f"&dataPortal=metagenome",
        json=[
            {"study_accession":"PRJEB51728","secondary_study_accession":"ERP136383","study_title":"Infant stool metagenomic datasets"},
        ],
        is_reusable=True, is_optional=True
    )
    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=study"
        f"&query=%22%28study_accession%3D{study_accession}+OR+secondary_study_accession%3D{study_accession}%29%22"
        f"&fields=study_accession"
        f"&limit="
        f"&format=json"
        f"&dataPortal=metagenome",
        json=[
            {"study_accession": study_accession}
        ],
        is_reusable=True, is_optional=True
    )

    httpx_mock.add_response(
        url=f"{EMG_CONFIG.ena.portal_search_api}?"
        f"result=read_run"
        f"&query=%22%28%28study_accession={study_accession}+OR+secondary_study_accession={study_accession}%29%20AND%20library_strategy=WGS%29%22"
        f"&limit=10000"
        f"&format=json"
        f"&fields=run_accession%2Csample_accession%2Csample_title%2Csecondary_sample_accession%2Cfastq_md5%2Cfastq_ftp%2Clibrary_layout%2Clibrary_strategy%2Clibrary_source%2Cscientific_name%2Chost_tax_id%2Chost_scientific_name%2Cinstrument_platform%2Cinstrument_model%2Clocation%2Clat%2Clon"
        f"&dataPortal=metagenome",
        json=[
            {"run_accession":"ERR10889188","sample_accession":"SAMEA112437737","sample_title":"stool","secondary_sample_accession":"ERS14548047","fastq_md5":"67613b1159c7d80eb3e3ca2479650ad5;6e6c5b0db3919b904a5e283827321fb9;41dc11f68009af8bc1e955a2c8d95d05","fastq_ftp":"ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/088/ERR10889188/ERR10889188.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/088/ERR10889188/ERR10889188_1.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/088/ERR10889188/ERR10889188_2.fastq.gz","library_layout":"PAIRED","library_strategy":"WGS","library_source":"METAGENOMIC","scientific_name":"human gut metagenome","host_tax_id":"","host_scientific_name":"","instrument_platform":"ILLUMINA","instrument_model":"Illumina HiSeq 2500","location":"19.754234 S 30.156915 E","lat":"-19.754234","lon":"30.156915"}
            ,
            {"run_accession":"ERR10889197","sample_accession":"SAMEA112437712","sample_title":"stool","secondary_sample_accession":"ERS14548022","fastq_md5":"323cd88dc2b3ccd17b47d8581a2be280;265ec70078cbdf9e92acd0ebf45aed8d;9402981ef0e93ea329bda73eb779d65c","fastq_ftp":"ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/097/ERR10889197/ERR10889197.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/097/ERR10889197/ERR10889197_1.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/097/ERR10889197/ERR10889197_2.fastq.gz","library_layout":"PAIRED","library_strategy":"WGS","library_source":"METAGENOMIC","scientific_name":"human gut metagenome","host_tax_id":"","host_scientific_name":"","instrument_platform":"ILLUMINA","instrument_model":"Illumina HiSeq 2500","location":"19.754234 S 30.156915 E","lat":"-19.754234","lon":"30.156915"}
            ,
            {"run_accession":"ERR10889214","sample_accession":"SAMEA112437729","sample_title":"stool","secondary_sample_accession":"ERS14548039","fastq_md5":"4dd2c869a5870984af3147cd8c34cca2;edc7298b1598848a71583718ff54ba8d;2aeeea4a32fae3318baff8bc7b3bc70c","fastq_ftp":"ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/014/ERR10889214/ERR10889214.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/014/ERR10889214/ERR10889214_1.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/014/ERR10889214/ERR10889214_2.fastq.gz","library_layout":"PAIRED","library_strategy":"WGS","library_source":"METAGENOMIC","scientific_name":"human gut metagenome","host_tax_id":"","host_scientific_name":"","instrument_platform":"ILLUMINA","instrument_model":"Illumina HiSeq 2500","location":"19.754234 S 30.156915 E","lat":"-19.754234","lon":"30.156915"}
            ,
            {"run_accession":"ERR10889221","sample_accession":"SAMEA112437721","sample_title":"stool","secondary_sample_accession":"ERS14548031","fastq_md5":"700d1ace4b6142dd32935039fb457618;3afa250c864c960324d424c523673f5f;a585727360a76bb22a948c30c5c41dd1","fastq_ftp":"ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/021/ERR10889221/ERR10889221.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/021/ERR10889221/ERR10889221_1.fastq.gz;ftp.sra.ebi.ac.uk/vol1/fastq/ERR108/021/ERR10889221/ERR10889221_2.fastq.gz","library_layout":"PAIRED","library_strategy":"WGS","library_source":"METAGENOMIC","scientific_name":"human gut metagenome","host_tax_id":"","host_scientific_name":"","instrument_platform":"ILLUMINA","instrument_model":"Illumina HiSeq 2500","location":"19.754234 S 30.156915 E","lat":"-19.754234","lon":"30.156915"}
            ,
        ],
        is_reusable=True, is_optional=True
    )

    # create fake results
    rawreads_folder = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/{study_accession}_rawreads_v6/abc123"
    )
    rawreads_folder.mkdir(exist_ok=True, parents=True)

    # Create CSV files for completed and failed assemblies
    with open(
        f"{rawreads_folder}/{EMG_CONFIG.rawreads_pipeline.completed_runs_csv}",
        "w",
    ) as file:
        for r in all_results:
            file.write(f"{r},all_results" + "\n")

    # Generate fake pipeline results for the successful assembly
    for r in all_results:
        generate_fake_rawreads_pipeline_results(
            rawreads_folder, r
        )

    # Pretend that a human resumed the flow with the biome picker
    BiomeChoices = Enum("BiomeChoices", {"root.engineered": "Root:Engineered"})
    UserChoices = get_users_as_choices()

    class AnalyseStudyInput(BaseModel):
        biome: BiomeChoices
        watchers: List[UserChoices]
        library_strategy_policy: Optional[ENALibraryStrategyPolicy]

    def suspend_side_effect(wait_for_input=None):
        if wait_for_input.__name__ == "AnalyseStudyInput":
            return AnalyseStudyInput(
                biome=BiomeChoices["root.engineered"],
                watchers=[UserChoices[admin_user.username]],
                library_strategy_policy=ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
            )

    mock_suspend_flow_run.side_effect = suspend_side_effect

    # RUN MAIN FLOW
    analysis_rawreads_study(study_accession=study_accession)

    # mock_start_cluster_job.assert_called()
    # mock_check_cluster_job_all_completed.assert_called()
    # mock_suspend_flow_run.assert_called()

    # samplesheet_table = Artifact.get("rawreads-v6-initial-sample-sheet")
    # assert samplesheet_table.type == "table"
    # table_data = json.loads(samplesheet_table.data)
    # assert len(table_data) == 1
    # assert table_data[0]["sample"] in [all_results]

    # study = analyses.models.Study.objects.get_or_create_for_ena_study(study_accession)
    # # assert (
    # #     study.analyses.filter(
    # #         assembly__ena_accessions__contains=[all_results]
    # #     ).count()
    # #     == 1
    # # )

    # # check biome and watchers were set correctly
    # assert study.biome.biome_name == "Engineered"
    # assert admin_user == study.watchers.first()

    # # Check that the study has v6 analyses
    # study.refresh_from_db()
    # assert study.features.has_v6_analyses

    # # Check taxonomies were imported
    # analysis_which_should_have_taxonomies_imported: analyses.models.Analysis = (
    #     analyses.models.Analysis.objects_and_annotations.get(
    #         assembly__sample__ena_sample__accession__contains=[all_results]
    #     )
    # )
    # assert (
    #     analyses.models.Analysis.TAXONOMIES
    #     in analysis_which_should_have_taxonomies_imported.annotations
    # )
    # assert (
    #     analyses.models.Analysis.TaxonomySources.UNIREF.value
    #     in analysis_which_should_have_taxonomies_imported.annotations[
    #         analyses.models.Analysis.TAXONOMIES
    #     ]
    # )
    # contig_taxa = analysis_which_should_have_taxonomies_imported.annotations[
    #     analyses.models.Analysis.TAXONOMIES
    # ][analyses.models.Analysis.TaxonomySources.UNIREF.value]
    # assert len(contig_taxa) == 10
    # assert (
    #     contig_taxa[0]["organism"]
    #     == "sk__Archaea;k__Thermoproteati;p__Nitrososphaerota;c__Nitrososphaeria;o__Nitrosopumilales;f__Nitrosopumilaceae;g__Nitrosopumilus;s__Candidatus Nitrosopumilus koreensis"
    # )

    # # Check functions were imported
    # go_slims = analysis_which_should_have_taxonomies_imported.annotations[
    #     analyses.models.Analysis.GO_SLIMS
    # ]

    # logging.warning(analysis_which_should_have_taxonomies_imported.annotations)

    # assert len(go_slims) == 4
    # assert go_slims[0] == "GO:0003824"

    # # Check files
    # workdir = Path(f"{EMG_CONFIG.slurm.default_workdir}/{study_accession}_v6")
    # assert workdir.is_dir()

    # assert study.external_results_dir == f"{study_accession[:-3]}/{study_accession}"

    # Directory(
    #     path=study.results_dir,
    #     glob_rules=[
    #         GlobHasFilesCountRule[4]
    #     ],  # taxonomy + goslim for the samplesheet, same two for the "merge" = 4
    # )
