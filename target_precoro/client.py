import backoff
import requests
import time
import hmac
import hashlib
import json
from requests.exceptions import JSONDecodeError as RequestsJSONDecodeError
from datetime import datetime, timezone
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from target_hotglue.client import HotglueSink


class PrecoroSink(HotglueSink):

    item_custom_fields = {}
    is_invoice_paid = False
    ACCOUNT_SETUP_STREAMS = {
        "suppliers",
        "documentcustomfields",
        "itemcustomfields",
    }

    ENTITY_TYPE_MAP = {
        "purchaseorders": 0,
        "receipts": 4,
        "invoices": 5,
        "expenses": 6,
        "payments": 7,
        "suppliers": 8,
        "items": 9,
        "locations": 10,
        "legalentities": 11,
        "taxes": 12,
        "companyusers": 13,
        "documentcustomfieldoptions": 14,
        "itemcustomfieldoptions": 15,
        "documentcustomfields": 16,
        "itemcustomfields": 17,
        "attachments": 18,
    }

    def _get_account_setup_map_field(self, record: dict) -> str:
        """Return the AccountSetup mapField value based on the current stream."""
        stream_map_fields = {
            "suppliers": "currency",
            "documentcustomfields": "code",
            "itemcustomfields": "code",
        }
        field_name = stream_map_fields.get(self.name)
        return record.get(field_name)

    def _get_account_setup_headers(self, account_setup: dict, payload: dict = None) -> dict:
        """Generate Authorization signature and headers for AccountSetup microservice."""
        secret = str(account_setup.get("secret"))
        company_id = str(account_setup.get("companyId", ""))

        # Signature must use compact JSON for non-GET and empty payload for GET.
        payload_json = json.dumps(payload, separators=(",", ":")) if payload is not None else ""

        string_to_sign = f"{payload_json}.{company_id}"
        signature = hmac.new(bytes(secret, 'UTF-8'), string_to_sign.encode(), hashlib.sha256).hexdigest()

        headers = {
            "X-PRECORO-AUTH": signature,
            "X-COMPANY-ID": company_id
        }
        return headers

    def _raise_account_setup_for_status(self, response: requests.Response, context: str) -> None:
        try:
            response.raise_for_status()
        except requests.HTTPError as err:
            self.logger.error(
                f"{context} failed: HTTP {response.status_code}. Response body: {response.text}"
            )
            raise err

    def _get_account_setup_id(self, payload: dict | None) -> str | None:
        if not payload:
            return None
        return payload.get("accountSetupId") or payload.get("externalId") or payload.get("id")

    def _build_account_setup_search_payload(
        self,
        integration_id: str,
        legal_entity_id,
        record: dict | None = None,
        custom_field_id=None,
    ) -> dict | None:
        account_setup = self.config.get("AccountSetup", {})
        integration_type = account_setup.get("integrationType")
        if not integration_type:
            self.logger.warning("Skipping AccountSetup search because integrationType is not configured.")
            return None

        payload = {
            "legalEntityId": int(legal_entity_id),
            "entityType": self.ENTITY_TYPE_MAP.get(self.name, 1),
            "integrationType": integration_type,
            "integrationId": integration_id,
        }

        record = record or {}
        map_field = self._get_account_setup_map_field(record)
        if map_field:
            payload["mapField"] = map_field

        name = record.get("name")
        if name:
            payload["name"] = name

        custom_field_id = custom_field_id if custom_field_id is not None else record.get("custom_field_id")
        if self._is_custom_field_option_stream() and custom_field_id is not None:
            payload["customFieldId"] = int(custom_field_id)

        if self.name == "suppliers" and "mapField" not in payload:
            self.logger.info("Skipping AccountSetup search because mapField is missing.")
            return None

        return payload

    def hit_account_setup_search(self, integration_id: str, record: dict, legal_entity_id) -> dict:
        account_setup = self.config.get("AccountSetup", {})
        base_url = account_setup.get("url", "").rstrip("/")
        if not base_url:
            self.logger.warning("AccountSetup URL is not configured.")
            return None

        if legal_entity_id is None:
            self.logger.info("Skipping AccountSetup search because legalEntityId is missing.")
            return None

        payload = self._build_account_setup_search_payload(integration_id, legal_entity_id, record=record)
        if not payload:
            return None
        
        url = f"{base_url}/api/hotglue/account_setup/search"
        headers = self._get_account_setup_headers(account_setup, payload)
        self.logger.info(f"POST {url} with payload: {payload}")
        response = requests.post(url, json=payload, headers=headers, timeout=15)
        self._raise_account_setup_for_status(response, "AccountSetup search")
        return response.json()

    def lookup_account_setup_record(
        self,
        source_external_id: str,
        legal_entity_id,
        custom_field_id=None,
    ) -> dict | None:
        if not source_external_id or legal_entity_id is None:
            return None

        fetch_resp = self.fetch_account_setup_records(source_external_id)
        if not fetch_resp or not fetch_resp.get("isSuccess"):
            return None

        records = fetch_resp.get("records", [])
        for record in records:
            if record.get("legalEntityId") != int(legal_entity_id):
                continue

            record_custom_field_id = record.get("customFieldId")
            if custom_field_id is not None and record_custom_field_id not in (None, int(custom_field_id)):
                continue

            return record

        return None

    def lookup_account_setup_id_by_source_identity(
        self,
        source_external_id: str,
        legal_entity_id,
        custom_field_id=None,
    ) -> str | None:
        record = self.lookup_account_setup_record(source_external_id, legal_entity_id, custom_field_id)
        return self._get_account_setup_id(record)

    def fetch_account_setup_records(self, external_id: str) -> dict:
        account_setup = self.config.get("AccountSetup", {})
        base_url = account_setup.get("url", "").rstrip("/")
        if not base_url:
            return None
        
        url = f"{base_url}/api/hotglue/account_setup"
        params = {"externalId": external_id}
        headers = self._get_account_setup_headers(account_setup, payload=None)
        
        self.logger.info(f"GET {url}?externalId={external_id}")
        response = requests.get(url, params=params, headers=headers, timeout=15)
        self._raise_account_setup_for_status(response, "AccountSetup fetch")
        return response.json()

    def hit_account_setup_patch(self, as_external_id: str, precoro_id, record: dict) -> dict:
        account_setup = self.config.get("AccountSetup", {})
        base_url = account_setup.get("url", "").rstrip("/")
        if not base_url:
            self.logger.warning("AccountSetup URL is not configured (Patch).")
            return None
            
        payload = {
            "entityId": precoro_id,
            "name": record.get("name"),
            "mapField": self._get_account_setup_map_field(record)
        }
        
        url = f"{base_url}/api/hotglue/account_setup/{as_external_id}"
        headers = self._get_account_setup_headers(account_setup, payload)
        self.logger.info(f"PATCH {url} with payload: {payload}")
        response = requests.patch(url, json=payload, headers=headers, timeout=15)
        self._raise_account_setup_for_status(response, "AccountSetup patch")
        return response.json()

    def _is_custom_field_option_stream(self) -> bool:
        return self.name in {"itemcustomfields", "documentcustomfields"}

    def _get_depend_add_endpoint(self) -> str | None:
        if self.name == "itemcustomfields":
            return "/itemcustomfields/options/depend_add"
        if self.name == "documentcustomfields":
            return "/documentcustomfields/options/depend_add"
        return None

    def _get_depend_add_option_id_param(self) -> str | None:
        if self.name == "itemcustomfields":
            return "item_custom_field_option_id"
        if self.name == "documentcustomfields":
            return "document_custom_field_option_id"
        return None

    def hit_custom_field_option_depend_add(self, option_id, legal_entity_ids: list[int]) -> dict:
        endpoint = self._get_depend_add_endpoint()
        option_id_param = self._get_depend_add_option_id_param()
        if not endpoint or not option_id_param:
            return None

        entity_ids_payload = (
            str(legal_entity_ids[0])
            if len(legal_entity_ids) == 1
            else f"[{', '.join(str(le) for le in legal_entity_ids)}]"
        )
        payload = {
            option_id_param: str(option_id),
            "depend_type": "1",
            "entity_ids": entity_ids_payload,
        }

        response = requests.patch(
            self.url(endpoint),
            data=payload,
            headers=self.default_headers,
            timeout=15,
        )
        self.validate_response(response)
        return response.json() if response.content else {}

    def is_account_setup_enabled(self, external_id, legal_entity_id) -> bool:
        return (
            self.name in self.ACCOUNT_SETUP_STREAMS
            and self.config.get("AccountSetup", {}).get("enabled", False)
            and external_id is not None
            and legal_entity_id is not None
        )

    def _apply_account_setup_logic(self, external_id: str, legal_entity_id, record: dict):
        """Handle fetching and mapping externalId via Account Setup microservice."""
        account_setup_ref_id = None
        all_legal_entity_ids = []

        self.logger.info(f"Triggering AccountSetup search for externalId: {external_id}")
        try:
            search_resp = self.hit_account_setup_search(external_id, record, legal_entity_id)
            self.logger.info(f"AccountSetup Search Response: {search_resp}")
            if search_resp and search_resp.get("isSuccess"):
                account_setup_ref_id = self._get_account_setup_id(search_resp)
                precoro_id = search_resp.get("precoroId")

                if precoro_id:
                    record["id"] = str(precoro_id)
                    self.logger.info(f"Found precoroId {precoro_id}. Setting record method to PUT.")

                    try:
                        fetch_resp = self.fetch_account_setup_records(account_setup_ref_id)
                        if fetch_resp and fetch_resp.get("isSuccess"):
                            records = fetch_resp.get("records", [])
                            all_legal_entity_ids = list(
                                {r.get("legalEntityId") for r in records if r.get("legalEntityId") is not None}
                            )
                            self.logger.info(f"Fetched Legal Entities for update: {all_legal_entity_ids}")
                    except Exception as exc:
                        self.logger.error(f"AccountSetup GET failed: {exc}")

        except Exception as exc:
            self.logger.error(f"AccountSetup Search failed: {exc}")

        return account_setup_ref_id, all_legal_entity_ids

    def prepare_account_setup_context(self, record: dict) -> dict:
        """Centralize AccountSetup preprocessing for record upserts."""
        external_id = record.pop("externalId", None)
        legal_entity_id = record.pop("legalEntityId", None)
        account_setup_enabled = self.is_account_setup_enabled(external_id, legal_entity_id)

        context = {
            "source_external_id": external_id,
            "external_id": external_id,
            "legal_entity_id": legal_entity_id,
            "account_setup_enabled": account_setup_enabled,
            "account_setup_ref_id": None,
            "all_legal_entity_ids": [],
        }

        if account_setup_enabled and external_id:
            account_setup_ref_id, all_legal_entity_ids = self._apply_account_setup_logic(
                external_id, legal_entity_id, record
            )
            context["account_setup_ref_id"] = account_setup_ref_id
            context["all_legal_entity_ids"] = all_legal_entity_ids
            if account_setup_ref_id:
                context["external_id"] = account_setup_ref_id

        self.apply_account_setup_record_fields(record, context)
        return context

    def apply_account_setup_record_fields(self, record: dict, context: dict) -> None:
        """Apply record mutations that are relevant only for AccountSetup flows."""
        if self.name != "suppliers" or not context.get("account_setup_enabled"):
            return

        all_legal_entity_ids = context.get("all_legal_entity_ids") or []
        legal_entity_id = context.get("legal_entity_id")
        if all_legal_entity_ids:
            record["supplierLegalEntityIds[]"] = [str(le) for le in all_legal_entity_ids]
        elif legal_entity_id:
            record["supplierLegalEntityIds[]"] = str(legal_entity_id)

    def _get_account_setup_legal_entity_ids(self, context: dict) -> list[int]:
        all_legal_entity_ids = context.get("all_legal_entity_ids") or []
        legal_entity_id = context.get("legal_entity_id")
        if all_legal_entity_ids:
            return [int(le) for le in all_legal_entity_ids]
        if legal_entity_id:
            return [int(legal_entity_id)]
        return []

    def find_custom_field_option_id(self, base_endpoint: str, external_id: str):
        if not base_endpoint or not external_id:
            return None

        try:
            response = self.request_api("GET", endpoint=base_endpoint)
            options = response.json().get("data", [])
            for option in options:
                if str(option.get("externalId")) == str(external_id):
                    option_id = option.get("id")
                    return str(option_id) if option_id is not None else None
        except Exception as exc:
            self.logger.warning(
                f"Failed to lookup existing custom field option for externalId {external_id} "
                f"at {base_endpoint}: {exc}"
            )
        return None

    def prepare_custom_field_account_setup_context(
        self,
        record: dict,
        context: dict,
        base_endpoint: str,
        custom_field_id: str,
    ) -> None:
        if not context.get("account_setup_enabled"):
            return

        external_id = context.get("external_id")
        if external_id:
            record["externalId"] = external_id

    def prepare_gl_parent_linkage(
        self,
        record: dict,
        context: dict,
        base_endpoint: str,
        custom_field_id: str,
        id_mapping: dict,
    ) -> None:
        parent_external_id = record.pop("parentExternalId", None)
        if not parent_external_id or record.get("parent[id]") or not context.get("account_setup_enabled"):
            if parent_external_id and not context.get("account_setup_enabled"):
                parent_id = id_mapping.get(custom_field_id, {}).get(parent_external_id)
                if parent_id:
                    record["parent[id]"] = parent_id
                else:
                    record["parentExternalId"] = parent_external_id
            return

        legal_entity_id = context.get("legal_entity_id")
        parent_account_setup_id = self.lookup_account_setup_id_by_source_identity(
            parent_external_id,
            legal_entity_id,
            custom_field_id=custom_field_id,
        )
        if not parent_account_setup_id:
            raise Exception(
                f"Parent {parent_external_id} not found in AccountSetup for legalEntityId {legal_entity_id}"
            )

        parent_id = id_mapping.get(custom_field_id, {}).get(parent_account_setup_id)
        if not parent_id:
            parent_id = self.find_custom_field_option_id(base_endpoint, parent_account_setup_id)

        if parent_id:
            record["parent[id]"] = parent_id
            return

        record["parentExternalId"] = parent_account_setup_id

    def apply_account_setup_dependencies(self, context: dict, precoro_id) -> None:
        """Apply extra Precoro dependency updates required by AccountSetup flows."""
        if not context.get("account_setup_enabled") or not self._is_custom_field_option_stream():
            return

        legal_entity_ids = self._get_account_setup_legal_entity_ids(context)
        if not legal_entity_ids:
            return

        self.logger.info(
            f"Triggering depend_add for {self.name} option {precoro_id} with legal entities {legal_entity_ids}"
        )
        depend_resp = self.hit_custom_field_option_depend_add(precoro_id, legal_entity_ids)
        self.logger.info(f"depend_add Response: {depend_resp}")

    def finalize_account_setup(self, context: dict, precoro_id, record: dict) -> None:
        """Patch AccountSetup record after Precoro entity has been created or updated."""
        if not context.get("account_setup_enabled"):
            return

        self.apply_account_setup_dependencies(context, precoro_id)

        account_setup_ref_id = context.get("account_setup_ref_id")
        if not account_setup_ref_id:
            return

        self.logger.info(
            f"Triggering AccountSetup patch for externalId {account_setup_ref_id} with precoroId {precoro_id}"
        )
        patch_resp = self.hit_account_setup_patch(account_setup_ref_id, precoro_id, record)
        self.logger.info(f"AccountSetup Patch Response: {patch_resp}")

    def should_patch_external_id(self, context: dict) -> bool:
        """Whether externalId should be patched after upsert instead of sent in the upsert payload."""
        return not (context.get("account_setup_enabled") and self._is_custom_field_option_stream())

    @property
    def base_url(self) -> str:
        url = self.config.get("base_url") or "https://api.precoro.com"
        if not url.startswith("https://"):
            url = f"https://{url}"
        return url

    @property
    def http_headers(self):
        auth_credentials = {
            "X-AUTH-TOKEN": self.config.get("auth_token"),
            "email": self.config.get("email"),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        return auth_credentials
    
    def _only_update_entries(self):
        """Config entries for only_update_existing_records (single source of truth)."""
        return self.config.get("only_update_existing_records") or []

    def _fetch_custom_field_list(self, endpoint: str, cache_attr: str) -> dict:
        """Fetch id -> name map from endpoint. Cached on target."""
        target = getattr(self, "_target", None)
        cached = getattr(target, cache_attr, None) if target else None
        if cached is not None:
            return cached
        data = self.request_api("GET", endpoint=endpoint).json().get("data", [])
        id_to_name = {str(i["id"]): (i.get("name") or i.get("title") or "").strip() for i in data if i.get("id") is not None}
        if target is not None:
            setattr(target, cache_attr, id_to_name)
        return id_to_name

    def _get_cf_id_to_name(self, is_icf: bool) -> dict:
        """Id -> name for item or document custom fields; fetches only if config needs it."""
        entries = self._only_update_entries()
        need = any(e.get("is_icf") for e in entries) if is_icf else any(e.get("is_dcf") for e in entries)
        if not need:
            return {}
        return self._fetch_custom_field_list(
            "/itemcustomfields" if is_icf else "/documentcustomfields",
            "_icf_id_to_name" if is_icf else "_dcf_id_to_name",
        )

    def is_only_update_existing_records(
        self, is_icf: bool = False, is_dcf: bool = False, custom_field_id: str = None
    ) -> bool:
        """True if this stream/record is in only_update_existing_records (skip new POSTs).
        Regular streams: table matches stream name. ICF/DCF: table is custom field name (resolved via API).
        """
        entries = self._only_update_entries()
        if not entries:
            return False

        def _normalize(s: str) -> str:
            return (s or "").strip().lower()

        stream_norm = _normalize(self.stream_name or getattr(self, "name", ""))

        if custom_field_id is not None and (is_icf or is_dcf):
            id_to_name = self._get_cf_id_to_name(is_icf)
            match_name = _normalize(id_to_name.get(str(custom_field_id)))
            if not match_name:
                return False
        else:
            match_name = stream_norm

        for e in entries:
            if e.get("is_dcf") != is_dcf or e.get("is_icf") != is_icf:
                continue
            if _normalize(e.get("table")) == match_name:
                return True
        return False

    allows_externalid = [
        "suppliers",
        "invoices",
        "purchaseorders",
        "payments",
        "taxes",
        "paymentterms",
        "itemcustomfields",
        "documentcustomfields",
        "items",
        "locations",
        "legalentities",
    ]

    def _handle_rate_limit(self, response):
        """Extracts RateLimit-Retry-After and handles rate limit based on type."""
        rate_limit_type = response.json().get("RateLimit-Type")
        if rate_limit_type == "Daily limiter":
            self.logger.error("Daily rate limit hit. Exiting.")
            raise FatalAPIError("Daily rate limit hit. Exiting.")
        
        if rate_limit_type == "Hourly limiter":
            self.logger.info(f"Hourly rate limit hit. Waiting for 3600 seconds.")
            time.sleep(3600)
        
        retry_after_str = response.json().get("RateLimit-Retry-After")
        if retry_after_str:
            retry_after_time = datetime.strptime(retry_after_str, "%Y-%m-%d %H:%M:%S %Z")
            retry_after_timestamp = retry_after_time.replace(tzinfo=timezone.utc).timestamp()
            current_timestamp = time.time()

            wait_time = retry_after_timestamp - current_timestamp

            if wait_time > 0:
                self.logger.info(f"Rate limit hit. Waiting for {wait_time:.2f} seconds until {retry_after_time}.")
                time.sleep(wait_time)

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.RequestException),
        max_tries=7,
        factor=3,
    )
    def _request(
        self, http_method, endpoint, params={}, request_data=None, headers={}, verify=True
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers.update(self.default_headers)
        params.update(self.params)
        data = request_data

        # forcing an error to test backoff
        # raise RetriableAPIError("Forcing a 429 error to test backoff behavior")

        response = requests.request(
            method=http_method, url=url, params=params, headers=headers, data=data, verify=verify
        )

        if response.status_code == 429:
            self.logger.warning("Received 429 Too Many Requests error")
            self.logger.warning(f"Response Headers: {response.headers}")
            self.logger.warning(f"Response Body: {response.text}")

            self._handle_rate_limit(response) 
        else:
            # we sleep for 1 second because that's their API rate limit
            time.sleep(1)

        self.validate_response(response)
        # if error is due to invoice fully paid, log the invoice is paid
        if self.is_invoice_paid:
            self.logger.info(f"Invoice with id {request_data['invoice[id]']}is fully paid.")
        return response

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        self.is_invoice_paid = False
        if response.status_code in [429] or 500 <= response.status_code < 600:
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            # ignore error of invoice being fully paid
            try:
                response_json = response.json()
            except (ValueError, RequestsJSONDecodeError):
                response_json = {}

            if isinstance(response_json, dict):
                invoice_payment_error = response_json.get("errors", {}).get("errors", {}).get("sumPaid", "")
            else:
                invoice_payment_error = ""
            if invoice_payment_error in ["The amount must be greater than 0 and not exceed 0", 'The Invoice is already fully paid.']:
                self.is_invoice_paid = True
                return
            try:
                msg = response.text
            except:
                msg = self.response_error_message(response)
            raise FatalAPIError(msg)
    
    def init_state(self):
        # get the full target state
        target_state = self._target._latest_state

        # If there is data for the stream name in target_state use that to initialize the state
        if target_state:
            if not self._state and target_state["bookmarks"].get(self.name) and target_state["summary"].get(self.name):
                self.latest_state = target_state
        # If not init sink state latest_state
        if not self.latest_state:
            self.latest_state = self._state or {"bookmarks": {}, "summary": {}}

        if self.name not in self.latest_state["bookmarks"]:
            if not self.latest_state["bookmarks"].get(self.name):
                self.latest_state["bookmarks"][self.name] = []

        if not self.summary_init:
            self.latest_state["summary"] = {}
            if not self.latest_state["summary"].get(self.name):
                self.latest_state["summary"][self.name] = {"success": 0, "fail": 0, "existing": 0, "updated": 0}

            self.summary_init = True


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
        
    def patch_external_id(self, pk, base_endpoint,externalId):
        if externalId and pk != "000000":
            try:
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
