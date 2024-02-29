import json
import logging

import requests


class KeboolaClientException(Exception):
    pass


class KeboolaClient:
    def __init__(self, token, url):
        self.token = token
        self.url = f'{url}/v2/storage'
        self.headers = {
            'X-StorageApi-Token': self.token,
            'Content-Type': 'application/json'
        }

    @staticmethod
    def _handle_http_error(response):
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            response_error = json.loads(e.response.text)
            raise KeboolaClientException(
                f"{response_error.get('error')}. Exception code {response_error.get('code')}") from e

    def get_trigger(self, flow_ids):
        # return all triggers that are in the flow_ids list
        return [trigger for trigger in self.get_triggers() if trigger.get('configurationId') in flow_ids]

    def get_triggers(self):
        url = f'{self.url}/triggers'
        response = requests.get(url=url, headers=self.headers)

        self._handle_http_error(response)
        return response.json()

    def get_component_configuration_detail(self, component_id, configuration_id):
        url = f'{self.url}/components/{component_id}/configs/{configuration_id}'
        response = requests.get(url=url, headers=self.headers)

        self._handle_http_error(response)
        return response.json()

    def get_table_detail(self, table_id):
        url = f'{self.url}/tables/{table_id}'
        response = requests.get(url=url, headers=self.headers)

        self._handle_http_error(response)
        return response.json()

    def remove_trigger(self, trigger_id):
        url = f'{self.url}/triggers/{trigger_id}'
        response = requests.delete(url=url, headers=self.headers)

        self._handle_http_error(response)
        logging.info(f"Trigger id:{trigger_id} deleted!")
        return response.text

    def create_trigger(self, trigger):
        url = f'{self.url}/triggers'
        response = requests.post(url=url, headers=self.headers, json=trigger)

        self._handle_http_error(response)
        logging.info(f"Trigger id:{response.json().get('id')} created!")
        return response.json()
