from __future__ import annotations

from django.db import models
from django.db.models import AutoField, CharField, JSONField

import ena.models


class MGnifyAutomatedModel(models.Model):
    """
    Base class for models that have an autoincrementing ID (perhaps for use as an accession)
    and an `is_ready` bool for when they should appear on website vs. in automation only.
    """

    id = AutoField(primary_key=True)
    is_ready = models.BooleanField(default=False)

    class Meta:
        abstract = True

    # TODO: suppression propagation


class VisibilityControlledModel(models.Model):
    """
    Base class for models that should inherit their privacy status (so visibility) from an ENA Study.
    """

    is_private = models.BooleanField(default=False)
    ena_study = models.ForeignKey(ena.models.Study, on_delete=models.CASCADE)
    webin_submitter = CharField(null=True, blank=True, max_length=25, db_index=True)

    class Meta:
        abstract = True


class ENADerivedModel(VisibilityControlledModel):

    ena_accessions = JSONField(default=list, db_index=True, blank=True)
    is_suppressed = models.BooleanField(default=False)

    # TODO – postgres GIN index on accessions?

    @property
    def first_accession(self):
        if len(self.ena_accessions):
            return self.ena_accessions[0]
        return None

    class Meta:
        abstract = True


class TimeStampedModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True
