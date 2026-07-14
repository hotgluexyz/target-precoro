"""Precoro target class."""

from typing import Type

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.sinks import Sink
from hotglue_singer_sdk.target_sdk.target import TargetHotglue

from target_precoro.sinks import (
    FallbackSink,
    ItemCustomFieldsSink
)


class TargetPrecoro(TargetHotglue):
    """Sample target for Precoro."""

    name = "target-precoro"
    SINK_TYPES = [FallbackSink, ItemCustomFieldsSink]
    config_jsonschema = th.PropertiesList(
        th.Property("auth_token", th.StringType, required=True),
        th.Property("email", th.StringType, required=True),
        th.Property(
            "AccountSetup",
            th.ObjectType(
                th.Property("enabled", th.BooleanType),
                th.Property("url", th.StringType),
                th.Property("secret", th.StringType),
                th.Property("companyId", th.StringType),
                th.Property("integrationType", th.StringType),
            ),
        ),
        th.Property(
            "only_update_existing_records",
            th.ArrayType(
                th.ObjectType(
                    th.Property("table", th.StringType, required=True),
                    th.Property("is_dcf", th.BooleanType, required=True),
                    th.Property("is_icf", th.BooleanType, required=True),
                )
            ),
            description="When set, records for listed tables are only updated (PUT); new records are skipped.",
        ),
    ).to_dict()

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        for sink_class in self.SINK_TYPES:
            if sink_class.name == stream_name:
                return sink_class
        return FallbackSink

if __name__ == "__main__":
    TargetPrecoro.cli()
