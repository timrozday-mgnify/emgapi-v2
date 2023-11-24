from django.db import models

import ena.models


# Some models associated with MGnify Analyses (MGYS, MGYA etc).


class MGnifyAccessionedModel(models.Model):
    accession = models.CharField(primary_key=True, max_length=20)

    class Meta:
        abstract = True


class Study(MGnifyAccessionedModel):
    ena_study = models.ForeignKey(ena.models.Study, on_delete=models.CASCADE)


class Sample(MGnifyAccessionedModel):
    ena_sample = models.ForeignKey(ena.models.Sample, on_delete=models.CASCADE)


class Analysis(MGnifyAccessionedModel):
    study = models.ForeignKey(Study, on_delete=models.CASCADE)
    results_dir = models.CharField(max_length=100)
