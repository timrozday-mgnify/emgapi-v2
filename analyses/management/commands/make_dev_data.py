from django.core.management import BaseCommand

from ena import models as ena_models
from analyses import models as analyses_models


class Command(BaseCommand):
    help = "Make some development data in the DB"

    def handle(self, *args, **options):
        study_directory = options.get("study_dir")
        self.make_biomes()
        ena_study = self.make_ena_study()
        mgnify_study = self.make_mgnify_study(ena_study)
        metaspades_assembler = self.make_assembler_metaspades()[0]
        megehit_assembler = self.make_assembler_megahit()[0]
        self.make_samples_and_reads(mgnify_study)
        self.make_assemblies(mgnify_study, metaspades_assembler, megehit_assembler)

    def make_biomes(self):
        root = analyses_models.Biome.objects.create(biome_name="root")
        host_assoc = analyses_models.Biome.objects.create_child(
            biome_name="Host-associated", parent=root
        )
        analyses_models.Biome.objects.create_child(biome_name="Engineered", parent=root)
        analyses_models.Biome.objects.create_child(
            biome_name="Human", parent=host_assoc
        )

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

    def make_assembler_metaspades(self):
        return analyses_models.Assembler.objects.get_or_create(
            name="metaspades", version="3.15.3"
        )

    def make_assembler_megahit(self):
        return analyses_models.Assembler.objects.get_or_create(
            name="megahit", version="1.2.9"
        )

    def make_assemblies(
        self,
        mgnify_study: analyses_models.Study,
        assembler_metaspades: analyses_models.Assembler,
        assembler_megahit: analyses_models.Assembler,
    ) -> [analyses_models.Assembly]:
        assembleable_runs = mgnify_study.runs.filter(
            experiment_type=analyses_models.Run.ExperimentTypes.METAGENOMIC
        )
        assembly_objects = []
        for run in assembleable_runs:
            assembly, _ = analyses_models.Assembly.objects.get_or_create(
                run=run,
                reads_study=mgnify_study,
                ena_study=mgnify_study.ena_study,
                assembler=assembler_metaspades,
                dir=f"/hps/tests/assembly_uploader",
                metadata={"coverage": 20},
            )
            assembly_objects.append(assembly)
        # create assembly for first run in list
        for run in assembleable_runs[:1]:
            assembly, _ = analyses_models.Assembly.objects.get_or_create(
                run=run,
                reads_study=mgnify_study,
                ena_study=mgnify_study.ena_study,
                assembler=assembler_megahit,
                dir=f"/hps/tests/assembly_uploader",
                metadata={"coverage": 20},
            )
            assembly_objects.append(assembly)

        return assembly_objects
