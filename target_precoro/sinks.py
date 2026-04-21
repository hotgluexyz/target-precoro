"""Precoro target sink class, which handles writing streams."""

from target_precoro.client import PrecoroSink


class ItemCustomFieldsSink(PrecoroSink):
    name = "itemcustomfields"

    # Used to map externalId to id
    # custom field id: {externalId : id}
    id_mapping = {}

    @property
    def endpoint(self):
        return f"/{self.stream_name}/custom_field_id/options"
    
    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        record = super().preprocess_record(record, context)
        custom_field_id = str(record.get("custom_field_id", None))
        parentExternalId = record.pop("parentExternalId", None)
        parentId = record.get("parent[id]", None)
        if parentExternalId and not parentId:
            parentId = self.id_mapping.get(custom_field_id, {}).get(parentExternalId, None)
            if parentId:
                record["parent[id]"] = parentId
                record.pop("parentExternalId", None)
            else:
                return {"error": f"Parent {parentExternalId} not found"}
        return record
        
    
    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        method = "POST"
        base_endpoint = self.endpoint
        if record.get("error"):
            raise Exception(record.get("error"))

        if record:
            externalId = record.pop("externalId", None)

            custom_field_id = record.pop("custom_field_id", None)
            if custom_field_id:
                custom_field_id = str(int(custom_field_id)) if isinstance(custom_field_id, float) else str(custom_field_id)
                base_endpoint = self.endpoint.replace(
                    "custom_field_id", custom_field_id
                )
            else:
                raise Exception("No custom field id provided for the record")
            
            endpoint = base_endpoint
            # Skip new records when only_update_existing_records applies
            id = record.pop("id", None)
            if not id and self.is_only_update_existing_records(
                is_icf=True, is_dcf=False, custom_field_id=custom_field_id
            ):
                state_updates["skipped"] = True
                return None, True, state_updates
            if id:
                id = int(id)
                method = "PUT"
                endpoint = f"{base_endpoint}/{id}"
            response = self.request_api(method, endpoint=endpoint, request_data=record)
            id = response.json()["id"]

            # update id mapping
            if custom_field_id not in self.id_mapping:
                self.id_mapping[custom_field_id] = {}
            self.id_mapping[custom_field_id][externalId] = id

            # patch record with externalId
            self.patch_external_id(id, base_endpoint, externalId)

            return id, True, state_updates


class FallbackSink(PrecoroSink):
    """Precoro target sink class."""

    @property
    def endpoint(self):
        endpoint = f"/{self.stream_name}"
        # if record is a custom field
        if self.name == "documentcustomfields":
            endpoint = f"{endpoint}/custom_field_id/options"
        return endpoint

    @property
    def name(self):
        return self.stream_name

    def check_and_fix_payment_amount(self, record: dict):
        """
            HGI-8258: fix payment amount if it's within 0.01 of the remaining amount
        """
        response = self.request_api("GET", endpoint=f"/invoices?id={record.get('invoice[id]')}")
        invoices = response.json().get("data", [])
        if len(invoices) == 0:
            raise Exception(f"Invoice {record.get('invoice[id]')} not found")
        
        invoice = invoices[0]
        remaining_amount = invoice.get("sum", 0) - float(invoice.get("sumPaid", 0))
        if abs(round((remaining_amount - record.get("sumPaid")), 2)) <= 0.01:
            record["sumPaid"] = remaining_amount

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        method = "POST"
        base_endpoint = self.endpoint
        if record.get("error"):
            raise Exception(record.get("error"))
        if record:
            externalId = record.pop("externalId", None)
            legalEntityId = record.pop("legalEntityId", None)
            account_setup_enabled = self.config.get("AccountSetup", {}).get("enabled", False)
            account_setup_ref_id = None
            
            if account_setup_enabled and externalId:
                try:
                    search_resp = self.hit_account_setup_search(externalId, record, legalEntityId)
                    if search_resp and search_resp.get("isSuccess"):
                        account_setup_ref_id = search_resp.get("externalId")
                        precoro_id = search_resp.get("precoroId")
                        if precoro_id:
                            # if Precoro record exists, we tell target it's an update (PUT)
                            record["id"] = str(precoro_id)
                except Exception as e:
                    self.logger.error(f"AccountSetup Search failed: {e}")

            custom_field_id = None
            if self.name == "documentcustomfields":
                custom_field_id = record.pop("custom_field_id", None)
                if custom_field_id:
                    custom_field_id = str(int(custom_field_id)) if isinstance(custom_field_id, float) else str(custom_field_id)
                    base_endpoint = self.endpoint.replace(
                        "custom_field_id", custom_field_id
                    )
                else:
                    raise Exception("No custom field id provided for the record")

            # Skip new records when only_update_existing_records applies
            id = record.pop("id", None)
            if not id and self.is_only_update_existing_records(
                is_icf=False,
                is_dcf=(self.name == "documentcustomfields"),
                custom_field_id=custom_field_id if self.name == "documentcustomfields" else None,
            ):
                state_updates["skipped"] = True
                return None, True, state_updates

            if self.name == "payments":
                self.check_and_fix_payment_amount(record)

            endpoint = base_endpoint
            if id:
                id = int(id)
                method = "PUT"
                endpoint = f"{base_endpoint}/{id}"
            response = self.request_api(method, endpoint=endpoint, request_data=record)
            # if invoice is fully paid return a dummy id so the job doesn't fail
            if self.is_invoice_paid:
                id = "000000"
                return id, True, state_updates
            else:
                id = response.json()["id"]
                idn = response.json().get("idn")
            pk = idn if self.name in ["invoices", "purchaseorders", "payments"] else id
            
            # Account Setup - Case 3 Step 2 (Patch microservice record)
            if account_setup_enabled and account_setup_ref_id:
                try:
                    self.hit_account_setup_patch(account_setup_ref_id, pk, record)
                except Exception as e:
                    self.logger.error(f"AccountSetup Patch failed: {e}")
                    raise

            self.patch_external_id(pk, base_endpoint, externalId)

            return id, True, state_updates
