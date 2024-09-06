import csv
import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Union

from django.db.models import QuerySet

logger = logging.getLogger(__name__)


@dataclass
class SamplesheetColumnSource:
    lookup_string: str
    renderer: Callable[[Any], str] = lambda x: x


def queryset_to_samplesheet(
    queryset: QuerySet,
    filename: Union[Path, str],
    column_map: dict[str, SamplesheetColumnSource] = None,
    bludgeon: bool = False,
) -> Path:
    """
    Write a nextflow samplesheet (TSV) where each row/sample is an object from a Django queryset.

    :param queryset: e.g. mymodel.objects.all()
    :param filename: e.g. mysamplesheet.tsv
    :param column_map: Maps columns of the sample sheet to a data source.  e.g.
        {
            "sample_id": SamplesheetColumnSource(lookup_string='id'),
            "start_time": SamplesheetColumnSource(lookup_string='my_other_model__created_at', renderer=lambda date: date[:4])
        }
    :param bludgeon: Boolean which if True will allow an existing sheet to be overwritten.
    :return: Path to the written samplesheet file.
    """

    _column_map = column_map or {}

    if not _column_map:
        for field in queryset.model._meta.get_fields():
            if not field.is_relation:
                _column_map[field.name] = SamplesheetColumnSource(
                    lookup_string=field.name
                )

    logger.info(f"Will write columns: {_column_map.keys()} to samplesheet")

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
        writer = csv.DictWriter(
            samplesheet, fieldnames=_column_map.keys(), delimiter="\t"
        )
        writer.writeheader()
        values_qs = queryset.values(
            *[source.lookup_string for source in _column_map.values()]
        )
        for obj in values_qs:
            writer.writerow(
                {
                    col: source.renderer(obj[source.lookup_string])
                    for col, source in _column_map.items()
                }
            )

    return _filename


def queryset_hash(queryset: QuerySet, field: str) -> str:
    """
    Make a unique(ish) hash of a queryset, based on concatenating the values of one field and hashing them.
    Useful e.g. for naming a queryset-based samplesheet.

    :param queryset: e.g. mymodel.objects.filter(started_at__lt='2022')
    :param field: e.g. "accession"
    :return: A string hash of e.g. mymodel.accession values.
    """
    vals = queryset.values_list(field, flat=True)
    vals_str = "".join(map(str, vals))
    return hashlib.md5(vals_str.encode()).hexdigest()
