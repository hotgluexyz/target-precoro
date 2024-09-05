import backoff
import requests
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
    
    @property
    def allows_externalid(self) -> list:
        allows_externalid = self.config.get("allows_externalid", [])
        if allows_externalid:
            if isinstance(allows_externalid, str):
                allows_externalid = allows_externalid.split(",")
                allows_externalid = [stream.lower() for stream in allows_externalid]
                allows_externalid = [stream.strip() for stream in allows_externalid]
            elif isinstance(allows_externalid, list):
                allows_externalid = [stream.lower() for stream in allows_externalid]
                allows_externalid = [stream.strip() for stream in allows_externalid]
            else:
                raise Exception(f"allows_externalid value in config is not valid, it should be a list of streams or a string of streams separated by a comma.")
        return allows_externalid

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params={}, request_data=None, headers={}
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers.update(self.default_headers)
        params.update(self.params)
        data = request_data

        response = requests.request(
            method=http_method, url=url, params=params, headers=headers, data=data
        )
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
