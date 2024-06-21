from unittest.mock import MagicMock, patch

from django.test import override_settings, TestCase

from app.targets.apis import DatasourceAPI, DestinationAPI


class TestDatasourceAPI(TestCase):
    """ Test the `DatasourceAPI` """

    @override_settings(TARGET_BACKEND_MAP={'DATASOURCE_URL': 'http://datasource.app/'})
    def setUp(self):
        self.api = DatasourceAPI()

    def test_url(self):
        actual = self.api.url
        expected = 'http://datasource.app/'
        self.assertEqual(actual, expected)

    @patch('app.targets.apis.requests.request')
    def test_list(self, mock_request):
        """
        Test the `.list()` method.
        """
        # Mock response
        mock_data = [{
            'id': 1,
            'hotel_id': 1,
            'room_id': '3fa85f64-5717-4562-b3fc-2c963f66afa6',
            'timestamp': '2024-06-11T14:00:00Z',
            'night_of_stay': '2024-06-12',
            'rpg_status': 1
        }]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_data

        # Mock requests return values
        mock_request.return_value = mock_response

        # Assert data
        actual = self.api.list()
        expected = mock_data
        self.assertEqual(actual, expected)

    @patch('app.targets.apis.requests.request')
    def test_upsert(self, mock_request):
        """
        Test the `.upsert()` method.
        """
        # Mock response
        mock_data = {
            'id': 1,
            'hotel_id': 1,
            'room_id': '3fa85f64-5717-4562-b3fc-2c963f66afa6',
            'timestamp': '2024-06-11T14:00:00Z',
            'night_of_stay': '2024-06-12',
            'rpg_status': 1
        }

        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = mock_data

        # Mock requests return values
        mock_request.return_value = mock_response

        # Assert data
        actual = self.api.upsert(mock_data)
        expected = mock_data
        self.assertEqual(actual, expected)


class TestDestinationAPI(TestCase):
    """ Test the `DestinationAPI` """

    @override_settings(TARGET_BACKEND_MAP={'DESTINATION_URL': 'http://destination.app/'})
    def setUp(self):
        self.api = DestinationAPI()

    def test_url(self):
        actual = self.api.url
        expected = 'http://destination.app/'
        self.assertEqual(actual, expected)

    @patch('app.targets.apis.requests.request')
    def test_upsert(self, mock_request):
        """
        Test the `.upsert()` method.
        """
        # Mock response
        mock_data = {
            'hotel_id': 1,
            'total': 10,
            'period_type': 'day',
            'period_start': '2024-06-13T23:21:38.248Z',
            'period_end': '2024-06-13T23:21:38.248Z'
        }

        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = mock_data

        # Mock requests return values
        mock_request.return_value = mock_response

        # Assert data
        actual = self.api.upsert(mock_data)
        expected = mock_data
        self.assertEqual(actual, expected)
