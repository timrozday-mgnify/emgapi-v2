import pytest

from emgapiv2.async_utils import anysync_property
from emgapiv2.log_utils import mask_sensitive_data


# Tests for async utils
class MyThing:
    def __init__(self):
        self.hello_to = "world"

    @property
    def message(self):
        return f"Hello {self.hello_to}"

    @anysync_property
    def any_message(self):
        return f"Hello {self.hello_to}"


def test_async_utils_anysync_property_works_in_sync_context():
    m = MyThing()
    assert m.message == "Hello world"
    assert m.any_message == "Hello world"


@pytest.mark.asyncio
async def test_async_utils_anysync_property_works_in_async_context():
    m = MyThing()
    assert m.message == "Hello world"
    assert await m.any_message == "Hello world"


def test_log_masking():
    script = "./run-command subcommand -flag=okay -password=verysecret"
    assert (
        mask_sensitive_data(script)
        == "./run-command subcommand -flag=okay -password=*****"
    )

    script = "./run-command subcommand -flag=okay -password='verysecret'"
    assert (
        mask_sensitive_data(script)
        == "./run-command subcommand -flag=okay -password='*****'"
    )

    script = './run-command subcommand -flag=okay -password="verysecret"'
    assert (
        mask_sensitive_data(script)
        == './run-command subcommand -flag=okay -password="*****"'
    )

    script = """
    ./run-command subcommand1 -flag=okay -password=verysecret"
    ./run-command subcommand2 -flag=okay -password=alsoverysecret"
    """
    assert (
        mask_sensitive_data(script)
        == """
    ./run-command subcommand1 -flag=okay -password=*****
    ./run-command subcommand2 -flag=okay -password=*****
    """
    )
