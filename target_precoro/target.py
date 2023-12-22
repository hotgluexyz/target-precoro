"""Precoro target class."""

from typing import Type

from singer_sdk import typing as th
from singer_sdk.sinks import Sink
from target_hotglue.target import TargetHotglue

from target_precoro.sinks import (
    FallbackSink
)


class TargetPrecoro(TargetHotglue):
    """Sample target for Precoro."""

    name = "target-precoro"
    SINK_TYPES = [FallbackSink]
    config_jsonschema = th.PropertiesList(
        th.Property("auth_token", th.StringType, required=True),
        th.Property("email", th.StringType, required=True),
    ).to_dict()

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        for sink_class in self.SINK_TYPES:
            return FallbackSink

if __name__ == "__main__":
    TargetPrecoro.cli()
