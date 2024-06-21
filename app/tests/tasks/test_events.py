import pandas as pd
from unittest.mock import patch, ANY

from dateutil.parser import parse
from model_bakery import baker

from django.test import TestCase

from app.models import EventLog
from app.tasks.events import (
    _load_events,
    _get_next_date_to_sync,
    _events_dataframe_to_payload_data,
    synchronize_events,
)


class TestEventTasks(TestCase):
    """ Test the event tasks and it's associated methods """

    def setUp(self):
        self.dataset = pd.DataFrame(data=[
            {
                'id': 1,
                'hotel_id': 1,
                'room_reservation_id': '3fa85f64-5717-4562-b3fc-2c963f66afa1',
                'event_timestamp': '2024-06-11T14:00:00Z',
                'night_of_stay': '2024-06-12',
                'status': 1
            },
            {
                'id': 3,
                'hotel_id': 1,
                'room_reservation_id': '3fa85f64-5717-4562-b3fc-2c963f66afa2',
                'event_timestamp': '2024-06-13T14:00:00Z',
                'night_of_stay': '2024-06-14',
                'status': 1
            },
            {
                'id': 2,
                'hotel_id': 1,
                'room_reservation_id': '3fa85f64-5717-4562-b3fc-2c963f66afa2',
                'event_timestamp': '2024-06-12T14:00:00Z',
                'night_of_stay': '2024-06-13',
                'status': 1
            },
        ])

    @patch('app.tasks.events.pd.read_csv')
    def test_load_events(self, mock_read_csv):
        """
        Test the `_load_events` method returns correct dataframe
        sorted in ascending order by the `event_timestamp` column.
        """
        mock_read_csv.return_value = self.dataset

        df_events = _load_events()

        # Assert event ids order
        actual_ids = [event['id'] for _, event in df_events.iterrows()]
        expected_ids = [1, 2, 3]
        self.assertListEqual(actual_ids, expected_ids)

        # Ensure the `event_date` column is populated on the fly
        actual_ids = [str(event['event_date']) for _, event in df_events.iterrows()]
        expected_ids = ['2024-06-11', '2024-06-12', '2024-06-13']
        self.assertEqual(actual_ids, expected_ids)

    @patch('app.tasks.events.pd.read_csv')
    def test_get_next_date_to_sync_1(self, mock_read_csv):
        """
        Test the `_get_next_date_to_sync` method returns the first timestamp
        from dataset when there is no event log history.
        """
        mock_read_csv.return_value = self.dataset

        df_events = _load_events()

        actual = _get_next_date_to_sync(df_events)
        expected = parse('2024-06-11T00:00:00Z').date()
        self.assertEqual(actual, expected)

    @patch('app.tasks.events.pd.read_csv')
    def test_get_next_date_to_sync_2(self, mock_read_csv):
        """
        Test the `_get_next_date_to_sync` method
        returns a correct next `event_date` from the dataframe.
        """
        mock_read_csv.return_value = self.dataset

        df_events = _load_events()

        synchronized_date = parse('2024-06-11T00:00:00Z').date()
        baker.make(EventLog, event_date=synchronized_date, is_success=True)

        actual = _get_next_date_to_sync(df_events)
        expected = parse('2024-06-12T00:00:00Z').date()
        self.assertEqual(actual, expected)

    @patch('app.tasks.events.pd.read_csv')
    def test_get_next_date_to_sync_3(self, mock_read_csv):
        """
        Test the `_get_next_date_to_sync` method return none
        when all events are synchronized.
        """
        mock_read_csv.return_value = self.dataset

        df_events = _load_events()

        synchronized_date = parse('2024-06-13T00:00:00Z').date()
        baker.make(EventLog, event_date=synchronized_date, is_success=True)

        self.assertIsNone(_get_next_date_to_sync(df_events))

    @patch('app.tasks.events.pd.read_csv')
    def test_get_next_date_to_sync_failed(self, mock_read_csv):
        """
        Test the `_get_next_date_to_sync` method raises `ValueError`
        when the last synchronized date is not present in the dataframe.
        """
        mock_read_csv.return_value = self.dataset

        df_events = _load_events()

        synchronized_date = parse('2024-06-10T00:00:00Z').date()
        baker.make(EventLog, event_date=synchronized_date, is_success=True)

        with self.assertRaises(ValueError) as err:
            _get_next_date_to_sync(df_events)

        actual = str(err.exception)
        expected = 'Last synchronized date is not found in the predefined data.'
        self.assertEqual(actual, expected)

    @patch('app.tasks.events.pd.read_csv')
    def test_events_dataframe_to_payload_data(self, mock_read_csv):
        """
        Test the `_events_dataframe_to_payload_data` converter method
        returns a correct request data.
        """
        mock_read_csv.return_value = self.dataset

        df_events = _load_events()

        actual = _events_dataframe_to_payload_data(df_events)
        expected = [
            {
                'id': 1,
                'hotel_id': 1,
                'room_id': '3fa85f64-5717-4562-b3fc-2c963f66afa1',
                'timestamp': '2024-06-11T14:00:00.000000Z',
                'rpg_status': 1,
                'night_of_stay': '2024-06-12'
            },
            {
                'id': 2,
                'hotel_id': 1,
                'room_id': '3fa85f64-5717-4562-b3fc-2c963f66afa2',
                'timestamp': '2024-06-12T14:00:00.000000Z',
                'rpg_status': 1,
                'night_of_stay': '2024-06-13'
            },
            {
                'id': 3,
                'hotel_id': 1,
                'room_id': '3fa85f64-5717-4562-b3fc-2c963f66afa2',
                'timestamp': '2024-06-13T14:00:00.000000Z',
                'rpg_status': 1,
                'night_of_stay': '2024-06-14'
            }
        ]
        self.assertListEqual(actual, expected)

    @patch('app.tasks.events.DatasourceAPI.upsert')
    @patch('app.tasks.events._get_next_date_to_sync')
    @patch('app.tasks.events.pd.read_csv')
    def test_synchronize_events(
        self,
        mock_read_csv,
        mock_get_next_date_to_sync,
        mock_api_upsert,
    ):
        next_date_to_sync = parse('2024-06-12').date()

        mock_read_csv.return_value = self.dataset
        mock_get_next_date_to_sync.return_value = next_date_to_sync

        synchronize_events()

        mock_api_upsert.assert_called_once_with({
            'id': 2,
            'hotel_id': 1,
            'room_id': ANY,
            'timestamp': '2024-06-12T14:00:00.000000Z',
            'rpg_status': 1,
            'night_of_stay': '2024-06-13'
        })

        # Assert the timestamp of last synchronization
        log = EventLog.objects.get()
        self.assertEqual(log.event_date, next_date_to_sync)
