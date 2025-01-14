from typing import Type

from django.core.exceptions import ValidationError as DjValidationError
from django.db import models
from pydantic import BaseModel
from pydantic import ValidationError as PydValidationError


class JSONFieldWithSchema(models.JSONField):
    """
    A Django JSONField for a model, except that the JSON content has a validator based on a pydantic schema.

    Usage:

    class MyThing(pydantic.BaseModel):
        name: str = pydantic.Field(..., description="Name of thing")
        length: Optional[int]

    class MyModel(django.db.models.Model):
        id = django.db.models.UUIDField(primary_key=True)
        things = JSONFieldWithSchema(schema=MyThing, is_list=True, strict=True)

    my_object = MyModel.objects.create(things=[{"name": "x-wing", "length": 1}])
    """

    def __init__(
        self,
        schema: Type[BaseModel] = None,
        is_list: bool = False,
        strict: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.schema = schema
        self.is_list = is_list
        self.is_strict = strict

    def validate(self, value, model_instance):
        super().validate(value, model_instance)

        # Use the Pydantic model for additional validation
        try:
            if self.is_list:
                if type(value) is not list:
                    raise DjValidationError("Value is not a list")
                [
                    self.schema.model_validate(item, strict=self.is_strict)
                    for item in value
                ]
            else:
                self.schema.model_validate(value, strict=self.is_strict)
        except PydValidationError as e:
            raise DjValidationError(f"Pydantic validation error: {e}")

        return value
