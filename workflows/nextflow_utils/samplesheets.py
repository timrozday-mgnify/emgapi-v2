import csv
import logging
from pathlib import Path
from typing import Union

from django.db.models import QuerySet


logger = logging.getLogger(__name__)


def queryset_to_samplesheet(
    queryset: QuerySet,
    filename: Union[Path, str],
    field_to_column_map: dict = None,
    bludgeon: bool = False,
) -> Path:
    if field_to_column_map is not None:
        column_names = list(field_to_column_map.values())
        field_names = list(field_to_column_map.keys())
    else:
        field_names = [
            field.name
            for field in queryset.model._meta.get_fields()
            if not field.is_relation
        ]
        column_names = field_names
    logger.info(f"Will write columns: {column_names} to samplesheet")

    _filename = Path(filename)
    folder = _filename.parent
    try:
        assert folder.is_dir()
        assert folder.exists()
    except AssertionError:
        raise Exception(f"The directory {folder} does not exist")
    else:
        logger.debug(f"Samplesheet's parent folder {folder} looks okay")
    if not bludgeon:
        try:
            assert not _filename.exists()
            logger.debug(f"Samplesheet filename {_filename} doesn't exist yet (good)")
        except AssertionError:
            raise Exception(
                f"The file {_filename} already exists and bludgeon=False so will not overwrite"
            )

    with open(_filename, "w", newline="") as samplesheet:
        writer = csv.DictWriter(samplesheet, fieldnames=column_names)
        writer.writeheader()
        values_qs = queryset.values(*field_names)
        for obj in values_qs:
            writer.writerow(
                {col: obj[fn] for fn, col in zip(field_names, column_names)}
            )

    return _filename
