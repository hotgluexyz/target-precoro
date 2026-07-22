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
        parentExternalId = record.get("parentExternalId", None)
        parentId = record.get("parent[id]", None)
        if parentExternalId and not parentId:
            if self.is_account_setup_enabled(record.get("externalId"), record.get("legalEntityId")):
                return record
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
            account_setup_context = self.prepare_account_setup_context(record)
            externalId = account_setup_context.get("external_id")

            custom_field_id = record.pop("custom_field_id", None)
            if custom_field_id:
                custom_field_id = str(int(custom_field_id)) if isinstance(custom_field_id, float) else str(custom_field_id)
                base_endpoint = self.endpoint.replace(
                    "custom_field_id", custom_field_id
                )
            else:
                raise Exception("No custom field id provided for the record")

            self.prepare_custom_field_account_setup_context(
                record,
                account_setup_context,
                base_endpoint,
                custom_field_id,
            )
            self.prepare_gl_parent_linkage(
                record,
                account_setup_context,
                base_endpoint,
                custom_field_id,
                self.id_mapping,
            )
            
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

            self.finalize_account_setup(account_setup_context, id, record)

            if self.should_patch_external_id(account_setup_context):
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
            account_setup_context = self.prepare_account_setup_context(record)
            externalId = account_setup_context.get("external_id")
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
                self.prepare_custom_field_account_setup_context(
                    record,
                    account_setup_context,
                    base_endpoint,
                    custom_field_id,
                )

            # Skip new records when only_update_existing_records applies
            id = record.pop("id", None)
            if not id and self.is_only_update_existing_records(
                is_icf=False,
                is_dcf=(self.name == "documentcustomfields"),
                custom_field_id=custom_field_id if self.name == "documentcustomfields" else None,
            ):
                state_updates["skipped"] = True
                return None, True, state_updates

            # Precoro's payments endpoint doesn't support PUT. Existing
            # payments must not be updated, so mark them as existing and skip the request.
            if id and self.name == "payments":
                self.logger.info(f"Skipping update for existing payment with id {id}")
                state_updates["existing"] = True
                return id, True, state_updates

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
            
            try:
                self.finalize_account_setup(account_setup_context, pk, record)
            except Exception as e:
                self.logger.error(f"AccountSetup Patch failed: {e}")
                raise

            if self.should_patch_external_id(account_setup_context):
                self.patch_external_id(pk, base_endpoint, externalId)

            return id, True, state_updates
