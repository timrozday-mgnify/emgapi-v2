import csv
import logging
from enum import Enum
from pathlib import Path
from typing import Type

from pydantic import BaseModel, ValidationError

from workflows.data_io_utils.file_rules.base_rules import FileRule


class CSVDelimiter(str, Enum):
    COMMA = ","
    TAB = "\t"


def generate_csv_schema_file_rule(
    row_schema: Type[BaseModel], delimiter: CSVDelimiter = CSVDelimiter.COMMA
) -> FileRule:
    """
    Generate a FileRule in which the rule test checks if the CSV file at path follows the specified schema.
    :param row_schema: A Pydantic model to validate each CSV row against
    :param delimiter: Field separator for CSV file. E.g. CSVDelimiter.COMMA
    :return: A FileRule that can be applied to any path
    """
    schema_name = row_schema.__class__.__name__

    def tester(path: Path):
        with path.open("r") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            reader.fieldnames = [
                field.lstrip("#").strip() for field in reader.fieldnames
            ]
            try:
                rows = [row_schema.model_validate(row) for row in reader]
            except ValidationError:
                return False
            else:
                logging.info(
                    f"{len(rows)} rows validated for {path} against {schema_name}"
                )
        return True

    return FileRule(
        rule_name=f"CSV should follow {schema_name} schema",
        test=tester,
    )
