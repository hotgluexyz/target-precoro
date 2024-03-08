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
    
    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        if not self.latest_state:
            self.init_state()

        hash = self.build_record_hash(record)

        existing_state =  self.get_existing_state(hash)

        if existing_state:
            return self.update_state(existing_state, is_duplicate=True)

        state = {"hash": hash}

        id = None
        success = False
        state_updates = dict()

        # paymentterms in Precoro has a field called externalId, so we're sending this in the payload
        if self.name != "paymentterms":
            external_id = record.pop("externalId", None)

        try:
            id, success, state_updates = self.upsert_record(record, context)
        except Exception as e:
            self.logger.exception(f"Upsert record error {str(e)}")
            state_updates['error'] = str(e)

        if success:
            self.logger.info(f"{self.name} processed id: {id}")

        state["success"] = success

        if id:
            state["id"] = id

        if not success and external_id:
            state["externalId"] = external_id

        if state_updates and isinstance(state_updates, dict):
            state = dict(state, **state_updates)

        self.update_state(state)
