import pytest

from workflows.ena_utils.ena_file_fetching import convert_ena_ftp_to_fire_fastq


def test_ena_fastq_ftp_to_fire_conversion():
    # should convert to FIRE
    ftp_url_naked = (
        "ftp.sra.ebi.ac.uk/vol1/fastq/ERR778/009/ERR7783799/ERR7783799_1.fastq.gz"
    )
    fire = convert_ena_ftp_to_fire_fastq(ftp_url_naked)
    assert fire == "s3://era-public/fastq/ERR778/009/ERR7783799/ERR7783799_1.fastq.gz"

    # should remove protocol and convert to FIRE
    ftp_url = (
        "ftp://ftp.sra.ebi.ac.uk/vol1/fastq/ERR778/009/ERR7783799/ERR7783799_1.fastq.gz"
    )
    fire = convert_ena_ftp_to_fire_fastq(ftp_url)
    assert fire == "s3://era-public/fastq/ERR778/009/ERR7783799/ERR7783799_1.fastq.gz"

    # should not change non-conformant URLs
    ftp_url = "ftp://somewhere.sra.else.ac.world/vol1/fastq/myfile.fastq.gz"
    fire = convert_ena_ftp_to_fire_fastq(ftp_url, raise_if_not_convertible=False)
    assert fire == "ftp://somewhere.sra.else.ac.world/vol1/fastq/myfile.fastq.gz"

    # should raise error for non-conformant URLs if asked
    with pytest.raises(Exception, match="not convertible to FIRE"):
        convert_ena_ftp_to_fire_fastq(ftp_url, raise_if_not_convertible=True)
