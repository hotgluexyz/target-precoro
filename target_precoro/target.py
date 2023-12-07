"""Precoro target class."""

from typing import Type

from singer_sdk import typing as th
from singer_sdk.sinks import Sink
from target_hotglue.target import TargetHotglue

from target_precoro.sinks import (
    ItemCustomFieldsOptions,
    Items,
    Payments,
    Suppliers,
    Taxes,
)


class TargetPrecoro(TargetHotglue):
    """Sample target for Precoro."""

    name = "target-precoro"
    SINK_TYPES = [Payments, Taxes, Items, Suppliers, ItemCustomFieldsOptions]
    config_jsonschema = th.PropertiesList(
        th.Property("auth_token", th.StringType, required=True),
        th.Property("email", th.StringType, required=True),
    ).to_dict()

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        for sink_class in self.SINK_TYPES:
            if stream_name == sink_class.name:
                return sink_class
        return ItemCustomFieldsOptions


if __name__ == "__main__":
    TargetPrecoro.cli()
