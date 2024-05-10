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
