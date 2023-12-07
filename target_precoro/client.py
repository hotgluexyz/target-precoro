import backoff
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from target_hotglue.client import HotglueSink


class PrecoroSink(HotglueSink):

    base_url = "https://api.precoro.com"
    item_custom_fields = {}

    @property
    def http_headers(self):
        auth_credentials = {
            "X-AUTH-TOKEN": self.config.get("auth_token"),
            "email": self.config.get("email"),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        return auth_credentials

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        method = "POST"
        endpoint = self.endpoint
        if record:
            id = record.get("id")
            if id:
                method = "PUT"
                endpoint = f"{endpoint}/{id}"
            response = self.request_api(method, endpoint=endpoint, request_data=record)
            id = response.json()["id"]
            return id, True, state_updates

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
        return response

    def map_custom_fields(self, record, payload):
        custom_fields = record.get("customFields")
        if custom_fields:
            [payload.update({cf.get("name"): cf.get("value")}) for cf in custom_fields]
        return payload
