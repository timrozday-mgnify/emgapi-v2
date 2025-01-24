import shlex
from pathlib import Path
from typing import Optional

import pandas as pd


def maybe_get_nextflow_trace_file(
    workdir: Path, command: str, check_existence: bool = True
) -> Optional[Path]:
    """
    Get the expected trace file location from a nextflow run comment, if one appears to exist.
    :param workdir: The working dir of nextflow
    :param command: The nextflow run command
    :param check_existence: Whether to check the file actually exists on the filesystem
    :return: The path to the trace file, if known/expected.
    """
    trace_file_location = None

    cli_args = shlex.split(command)
    cli_args = {
        arg: True if param.startswith("-") else param
        for arg, param in zip(cli_args, cli_args[1:] + ["--"])
        if arg.startswith("-")
    }
    if "-with-trace" in cli_args:
        print("Expecting a trace file because command includes `with-trace`")
        trace_file_location = Path(cli_args["-with-trace"])

        if not trace_file_location.is_absolute():
            trace_file_location = workdir / trace_file_location

        print(f"Trace file should be at {trace_file_location}")

    if trace_file_location and check_existence:
        if not trace_file_location.exists():
            print(
                f"Expected a trace file at {trace_file_location}, but it does not exist"
            )
            return None

    return trace_file_location


def maybe_get_nextflow_trace_df(workdir: Path, command: str) -> Optional[pd.DataFrame]:
    """
    Get the trace file content of a nextflow run comment, if one appears to exist.

    :param workdir: The working dir of nextflow
    :param command: The nextflow run command
    :return: The trace file content as a pandas dataframe.
    """
    trace_file_location = maybe_get_nextflow_trace_file(workdir, command)
    if trace_file_location is not None:
        print("Reading trace file into dataframe...")
        return pd.read_csv(trace_file_location, sep="\t")
