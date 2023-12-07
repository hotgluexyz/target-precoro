"""Precoro target sink class, which handles writing streams."""

from target_precoro.client import PrecoroSink


class Payments(PrecoroSink):
    """Precoro target sink class."""

    endpoint = "/payments"
    name = "Payments"

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        payload = {
            "id": record.get("externalId"),
            "invoice[id]": record.get("invoiceNumber"),
            "sumPaid": record.get("amount"),
            "payDate": record.get("payDate"),
        }
        payload = self.map_custom_fields(record, payload)
        return payload


class Taxes(PrecoroSink):
    """Precoro target sink class."""

    endpoint = "/taxes"
    name = "TaxCodes"

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        payload = {
            "name": record.get("name"),
            "value": record.get("percentage"),
            "percent": record.get("percentage"),
        }
        payload = self.map_custom_fields(record, payload)
        return payload


class Items(PrecoroSink):
    """Precoro target sink class."""

    endpoint = "/items"
    name = "Items"

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        payload = {
            "sku": record.get("sku"),
            "name": record.get("name"),
            "description": record.get("name"),
            "price": record.get("price"),
            "type": record.get("type"),
            "unit": record.get("unit"),
            "category[id]": record.get("category"),
        }
        payload = self.map_custom_fields(record, payload)
        return payload


class Suppliers(PrecoroSink):
    """Precoro target sink class."""

    endpoint = "/suppliers"
    name = "Vendors"

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        address = record.get("addresses", [])
        phone = record.get("phoneNumbers", [])
        payload = {
            "name": record.get("vendorName"),
            "currencies[]": record.get("currency"),
            "uniqueCode": record.get("vendorNumber"),
        }
        if address:
            add = address[0]
            payload["legalAddress"] = add.get("line1")
            payload["city"] = add.get("city")
            payload["state"] = add.get("state")
            payload["postalCode"] = add.get("postalCode")
            payload["country"] = add.get("country")

        if phone:
            phone = phone[0]
            payload["phone"] = phone.get("number")
        payload = self.map_custom_fields(record, payload)
        return payload


class ItemCustomFieldsOptions(PrecoroSink):
    """Precoro target sink class."""

    endpoint = "/itemcustomfields/custom_field_id/options"
    @property
    def name(self):
        return self.stream_name

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        payload = {}
        custom_field_name = self.stream_name
        if not self.item_custom_fields:
            custom_fields = self.request_api("GET", endpoint="/itemcustomfields")
            custom_fields = custom_fields.json().get("data", [])
            [
                self.item_custom_fields.update({cf.get("name"): cf.get("id")})
                for cf in custom_fields
            ]

        custom_field_id = self.item_custom_fields.get(custom_field_name)
        if custom_field_id:
            self.endpoint = self.endpoint.replace("custom_field_id", str(custom_field_id))
            payload = self.map_custom_fields(record, payload)
        return payload
