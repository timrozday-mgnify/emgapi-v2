#!/usr/bin/env nextflow

params.sample = "SAMN10879440"

filereport = file("https://www.ebi.ac.uk/ena/portal/api/filereport?accession=${params.sample}&result=read_run&fields=run_accession,fastq_ftp")

process getReadFiles {

    input:
    path 'read_report.tsv' from filereport

    output:
    tuple val(run_accession), val(fastq_url) into fastqs

    """
    cat read_report.tsv | cut -f1 -f2 | tail -n +2
    """

}

process downloadFastq {

    input:
    tuple val(run_accession), val(fastq_url) from fastqs

    output:
    path "${run_accession}.fastq"

    """
    curl -sS \${fastq_url} -o \${run_accession}.fastq
    """
}
