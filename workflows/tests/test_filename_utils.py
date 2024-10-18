from pathlib import Path

import pytest

from workflows.data_io_utils.filenames import (
    accession_prefix_separated_dir_path,
    file_path_shortener,
)


def test_file_path_shortener():
    assert file_path_shortener("my_file.csv", 1, 20) == "my_file.csv"

    assert file_path_shortener("my_file.csv", 2, 5) == "m..sv"
    assert file_path_shortener("my_file.csv", 2, 6) == "m..csv"

    assert file_path_shortener("/path/to/my_file.csv", 1, 11) == "/p/t/my_file.csv"
    assert file_path_shortener("/path/to/my_file.csv", 2, 10) == "/pa/to/m..ile.csv"

    assert (
        file_path_shortener("/path/to/my_file.csv", 3, 11, True) == "pat_to_my_file_csv"
    )
    assert (
        file_path_shortener("/path/to/my_very_long_file.csv", 3, 10, True)
        == "pat_to_m_ile_csv"
    )


def test_accession_prefix_separated_dir_path():
    assert accession_prefix_separated_dir_path("PRJ123456", 5, 6) == Path(
        "PRJ12/PRJ123/PRJ123456"
    )
    assert accession_prefix_separated_dir_path("PRJ123456", 5, 6, 99) == Path(
        "PRJ12/PRJ123/PRJ123456/PRJ123456"
    )
    assert accession_prefix_separated_dir_path("ERR1", 9, 9) == Path("ERR1/ERR1/ERR1")
    with pytest.raises(AssertionError):
        accession_prefix_separated_dir_path("PRJ1", 0, 5)
    assert accession_prefix_separated_dir_path("PRJ123456", 3, 6, 7) == Path(
        "PRJ/PRJ123/PRJ1234/PRJ123456"
    )
