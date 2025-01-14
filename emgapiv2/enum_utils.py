from enum import Enum


class DjangoChoicesCompatibleStrEnum(str, Enum):
    # TODO: after python 3.11 adopted everywhere switch str, Enum -> StrEnum
    """
    This is a str Enum that can also be easily converted to a Django Choices object.
    """

    @classmethod
    def as_choices(cls):
        return [(item.value, item.name) for item in cls]
