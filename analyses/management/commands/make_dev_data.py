from django.core.management import BaseCommand

from ena import models as ena_models
from analyses import models as analyses_models


class Command(BaseCommand):
    help = "Make some development data in the DB"

    def handle(self, *args, **options):
        study_directory = options.get("study_dir")
        ena_study = self.make_ena_study()
        mgnify_study = self.make_mgnify_study(ena_study)
        self.make_samples_and_reads(mgnify_study)
        self.make_assemblies(mgnify_study)

    def make_ena_study(self) -> ena_models.Study:
        ena_study, _ = ena_models.Study.objects.get_or_create(
            accession="PRJNA398089",
            defaults={
                "title": "Longitudinal Multi’omics of the Human Microbiome in Inflammatory Bowel Disease",
                "additional_accessions": ["SRP115494"],
            },
        )
        return ena_study

    def make_mgnify_study(self, ena_study: ena_models.Study) -> analyses_models.Study:
        mgnify_study, _ = analyses_models.Study.objects.get_or_create(
            ena_study=ena_study,
            defaults={
                "title": "Longitudinal Multi’omics of the Human Microbiome in Inflammatory Bowel Disease",
            },
        )
        return mgnify_study

    def make_samples_and_reads(
        self, mgnify_study: analyses_models.Study
    ) -> [analyses_models.Run]:
        runs = ["SRR6180434", "SRR6704248"]
        samples = ["SAMN07793787", "SAMN08514017"]
        types = [
            analyses_models.Run.ExperimentTypes.METAGENOMIC.value,
            analyses_models.Run.ExperimentTypes.AMPLICON.value,
        ]

        run_objects = []
        for run, sample, type in zip(runs, samples, types):
            ena_sample, _ = ena_models.Sample.objects.get_or_create(
                accession=sample, study=mgnify_study.ena_study
            )
            mg_sample, _ = analyses_models.Sample.objects.get_or_create(
                ena_sample=ena_sample, ena_study=mgnify_study.ena_study
            )
            run, _ = analyses_models.Run.objects.get_or_create(
                ena_accessions=[run],
                study=mgnify_study,
                sample=mg_sample,
                ena_study=mgnify_study.ena_study,
                experiment_type=type,
            )
            run_objects.append(run)
        return run_objects

    def make_assemblies(
        self, mgnify_study: analyses_models.Study
    ) -> [analyses_models.Assembly]:
        assembleable_runs = mgnify_study.runs.filter(
            experiment_type=analyses_models.Run.ExperimentTypes.METAGENOMIC
        )
        assembly_objects = []
        for run in assembleable_runs:
            assembly, _ = analyses_models.Assembly.objects.get_or_create(
                run=run, study=mgnify_study, ena_study=mgnify_study.ena_study
            )
            assembly_objects.append(assembly)
        return assembly_objects
