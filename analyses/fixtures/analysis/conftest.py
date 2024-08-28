import pytest

import django

django.setup()

import analyses.models as mg_models

versions = {"metaspades": "3.15.3", "spades": "3.15.3", "megahit": "1.2.9"}


@pytest.fixture
def raw_read_analyses(raw_read_run):
    mgyas = []
    for run in raw_read_run:
        mgya, _ = mg_models.Analysis.objects_and_annotations.get_or_create(
            run=run,
            sample=run.sample,
            study=run.study,
            ena_study_id=run.ena_study_id,
        )
        mgya.annotations[mg_models.Analysis.PFAMS] = [
            {"count": 1, "description": "PFAM1"}
        ]
        mgya.save()
        mgyas.append(mgya)

    mgyas[0].status[mg_models.Analysis.AnalysisStates.ANALYSIS_STARTED] = True
    mgyas[0].status[mg_models.Analysis.AnalysisStates.ANALYSIS_COMPLETED] = True
    mgyas[1].status[mg_models.Analysis.AnalysisStates.ANALYSIS_STARTED] = True
    mgyas[0].save()
    mgyas[1].save()

    return mgyas
