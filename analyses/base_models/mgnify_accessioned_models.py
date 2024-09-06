from __future__ import annotations

from django.db import models
from django.db.models import CharField, F, Value
from django.db.models.functions import Cast, LPad


class ConcatOp(models.Func):
    # TODO: remove after Django 5.1
    arg_joiner = " || "
    function = None
    output_field = models.TextField()
    template = "%(expressions)s"


class MGnifyAccessionField(models.GeneratedField):
    accession_prefix: str = None
    accession_length: int = None

    """
    A special type of GeneratedField, that uses the models `id` to form a column of accessions.
    The accessions are prefixed (e.g. MGYA) and zero padded (e.g. MGYA000001).
    The accession are persisted to the DB (stored as a column) and unique and indexed (for use as lookups e.g. as a key).
    """

    def __init__(self, accession_prefix: str, accession_length: int, **kwargs):
        self.accession_prefix = accession_prefix
        self.accession_length = accession_length
        for dont_pass in [
            "expression",
            "output_field",
            "db_index",
            "db_persist",
            "unique",
        ]:
            kwargs.pop(dont_pass, None)

        super().__init__(
            expression=ConcatOp(
                Value(accession_prefix),
                LPad(Cast(F("id"), CharField()), accession_length, Value("0")),
            ),
            output_field=CharField(
                max_length=(accession_length + len(accession_prefix) + 2)
            ),
            db_persist=True,
            db_index=True,
            unique=True,
            **kwargs,
        )

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        kwargs["accession_prefix"] = self.accession_prefix
        kwargs["accession_length"] = self.accession_length
        return name, path, args, kwargs
