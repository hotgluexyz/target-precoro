"""Precoro target sink class, which handles writing streams."""

from target_precoro.client import PrecoroSink
from typing import Dict, List, Optional
from singer_sdk.plugin_base import PluginBase

class FallbackSink(PrecoroSink):
    """Precoro target sink class."""

    @property
    def endpoint(self):
        endpoint = f"/{self.stream_name}"
        # if record is a custom field
        if self.name in ["itemcustomfields", "documentcustomfields"]:
            endpoint = f"{endpoint}/custom_field_id/options"
        return endpoint

    @property
    def name(self):
        return self.stream_name

    def get_default_values(self):
        res = {}
        default_values = self.config.get("default_values", [])
        # keep only values for current sink
        sink_default_values = [value for value in default_values if value.get("stream","").lower() == self.name]
        if sink_default_values:
            for value in sink_default_values:
                field = value.get("field")
                val = value.get("value")
                val_type = value.get("type")
                if field and val and val_type:
                    if val_type == "int":
                        val = int(val)
                    elif val_type == "float":
                        val = float(val)
                    elif val_type in ["bool", "boolean"]:
                        val = bool(val_type)
                    res[field] = val
        return res

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        try:
            # get default values from config
            default_fields = self.get_default_values()
            record.update(default_fields)
            return record
        except Exception as e:
            return {"error": f"Failed during preprocessing record with error: {str(e)}"}

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        method = "POST"
        base_endpoint = self.endpoint
        if record.get("error"):
            raise Exception(record.get("error"))
        if record:
            externalId = record.pop("externalId", None)
            # if record is a custom field
            if self.name in ["itemcustomfields", "documentcustomfields"]:
                custom_field_id = record.pop("custom_field_id", None)
                if custom_field_id:
                    custom_field_id = str(int(custom_field_id)) if isinstance(custom_field_id, float) else str(custom_field_id)
                    base_endpoint = self.endpoint.replace(
                        "custom_field_id", custom_field_id
                    )
                else:
                    raise Exception("No custom field id provided for the record")
            endpoint = base_endpoint
            # post or put record
            id = record.pop("id", None)
            if id:
                id = int(id)
                method = "PUT"
                endpoint = f"{base_endpoint}/{id}"
            response = self.request_api(method, endpoint=endpoint, request_data=record)
            # if invoice is fully paid return a dummy id so the job doesn't fail
            if self.is_invoice_paid:
                id = "000000"
            else:
                id = response.json()["id"]
                idn = response.json().get("idn")
            # patch record with externalId
            if externalId and id != "000000":
                try:
                    pk = idn if self.name in ["invoices", "purchaseorders", "payments"] else id
                    externalid_endpoint = f"{base_endpoint}/{pk}"
                    external_id_payload = {"externalId": externalId}
                    headers = {"Content-Type": "application/x-www-form-urlencoded"}
                    response = self.request_api(
                        "PATCH",
                        endpoint=externalid_endpoint,
                        request_data=external_id_payload,
                        headers=headers,
                    )
                except Exception as e:
                    raise Exception(
                        f"Failed while trying to send externalId {externalId}, error: {e}"
                    )

            return id, True, state_updates
