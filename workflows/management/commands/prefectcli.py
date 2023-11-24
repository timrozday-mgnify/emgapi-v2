from django.core.management.base import BaseCommand
from prefect.cli import app


class Command(BaseCommand):
    help = "Run the Prefect CLI"

    def run_from_argv(self, argv):
        app(argv[2:])
