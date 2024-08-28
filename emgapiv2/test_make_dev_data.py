import pytest
from django.core.management import call_command

from analyses.models import Biome


@pytest.mark.dev_data_maker
@pytest.mark.django_db(transaction=True)
def test_make_dev_data(top_level_biomes, assemblers, raw_read_analyses):
    """
    Dummy test that just sets up fixtures and dumps them to JSON for using as dev data.
    """

    assert Biome.objects.count() == 4

    call_command("dumpdata", "-o", "dev-db.json", "--indent", "2")
