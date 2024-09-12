import pytest

from workflows.data_io_utils.filenames import file_path_shortener


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
        == "pat_to_m__ile_csv"
    )
