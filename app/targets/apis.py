import requests

from django.conf import settings
from typing import Dict, List


class APIClient:

    def __init__(self, url):
        self.url = url

    def _api_request(
        self,
        method: str,
        path: str,
        payload: any = None,
        headers: Dict = {},
        params: Dict = {}
    ) -> requests.Response:
        url = self.url + path
        req_headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }

        if headers:
            req_headers.update(headers)

        response = requests.request(method, url, json=payload, headers=req_headers, params=params)
        response.raise_for_status()

        return response


class DatasourceAPI(APIClient):

    def __init__(self):
        url = settings.TARGET_BACKEND_MAP['DATASOURCE_URL']

        super().__init__(url)

    def list(self, params: Dict = {}) -> List[Dict]:
        """
        Upsert a specific event with ID to the data provider service database.
        """
        method = 'GET'
        path = 'events/'

        response = self._api_request(method, path, params=params)

        return response.json()

    def upsert(self, event: Dict) -> Dict:
        """
        Upsert a specific event with ID to the data provider service database.
        """
        method = 'POST'
        path = 'events/'

        response = self._api_request(method, path, event)

        return response.json()


class DestinationAPI(APIClient):

    def __init__(self):
        url = settings.TARGET_BACKEND_MAP['DESTINATION_URL']

        super().__init__(url)

    def upsert(self, reservation: Dict) -> Dict:
        """
        Upsert total reservation at particular hotel on a specific period
        to the data view service database.
        """
        method = 'POST'
        path = 'reservations/'

        response = self._api_request(method, path, reservation)

        return response.json()
