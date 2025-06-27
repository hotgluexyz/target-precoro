import backoff
import requests
import time
from datetime import datetime, timezone
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from target_hotglue.client import HotglueSink


class PrecoroSink(HotglueSink):

    item_custom_fields = {}
    is_invoice_paid = False

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
            invoice_payment_error = response.json().get("errors", {}).get("errors", {}).get("sumPaid", "")
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

