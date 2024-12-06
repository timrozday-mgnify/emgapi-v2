import logging

from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from django.db import models

# Some models that mirror ENA objects, like Study, Sample, Run etc


class ENAModel(models.Model):

    accession = models.CharField(primary_key=True, max_length=20)
    fetched_at = models.DateTimeField(auto_now=True)
    additional_accessions = models.JSONField(default=list)
    # webin, status etc

    class Meta:
        abstract = True


class StudyManager(models.Manager):
    async def get_ena_study(self, ena_study_accession):
        logging.info(f"Will get ENA study for {ena_study_accession} from DB")
        ena_study = False
        try:
            ena_study = (
                await self.get_queryset()
                .filter(
                    models.Q(accession=ena_study_accession)
                    | models.Q(additional_accessions__icontains=ena_study_accession)
                )
                .afirst()
            )
            logging.debug(f"Got {ena_study}")
        except (MultipleObjectsReturned, ObjectDoesNotExist) as e:
            logging.warning(
                f"Problem getting ENA study {ena_study_accession} from ENA models DB"
            )
        return ena_study


class Study(ENAModel):
    title = models.TextField()

    class Meta:
        verbose_name_plural = "studies"

    objects: StudyManager = StudyManager()

    def __str__(self):
        return self.accession


class Sample(ENAModel):
    metadata = models.JSONField(default=dict)
    study = models.ForeignKey(Study, on_delete=models.CASCADE, related_name="samples")

    def __str__(self):
        return self.accession
