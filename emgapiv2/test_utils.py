import pytest

from emgapiv2.async_utils import anysync_property


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
