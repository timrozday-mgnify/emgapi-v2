from django.db import models


# Some models that mirror ENA objects, like Study, Sample, Run etc


class ENAModel(models.Model):
    accession = models.CharField(primary_key=True, max_length=20)
    fetched_at = models.DateTimeField(auto_now=True)
    additional_accessions = models.JSONField(default=list)
    # webin, status etc

    class Meta:
        abstract = True


class Study(ENAModel):
    title = models.TextField()


class Sample(ENAModel):
    metadata = models.JSONField(default=dict)
    study = models.ForeignKey(Study, on_delete=models.CASCADE, related_name="samples")
