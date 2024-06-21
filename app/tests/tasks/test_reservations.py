from unittest.mock import patch, ANY

from dateutil.parser import parse
from model_bakery import baker

from django.test import TestCase

from app.models import ReservationLog
from app.tasks.reservations import (
    _day_period_of,
    _month_period_of,
    _year_period_of,
    _get_timestamp_to_sync,
    _get_period_from,
    _count_events_on,
    _synchronize,
    synchronize_daily_reservations,
    synchronize_monthly_reservations,
    synchronize_yearly_reservations,
)


class TestReservationTasks(TestCase):
    """ Test the reservation tasks and its associated methods """

    def test_day_period_of(self):
        """
        Test the `_day_period_of` method returns correct daily period.
        """
        timestamp = parse('2024/06/14T10:15:59')

        actual = _day_period_of(timestamp)
        expected = (
            parse('2024/06/14T00:00:00'),
            parse('2024/06/14T23:59:59.999999')
        )
        self.assertEqual(actual, expected)

    def test_month_period_of(self):
        """
        Test the `_month_period_of` method returns correct monthly period.
        """
        timestamp = parse('2024/06/14T10:15:59')

        actual = _month_period_of(timestamp)
        expected = (
            parse('2024/06/01T00:00:00'),
            parse('2024/06/30T23:59:59.999999')
        )
        self.assertEqual(actual, expected)

    def test_year_period_of(self):
        """
        Test the `_year_period_of` method returns correct yearly period.
        """
        timestamp = parse('2024/06/14T10:15:59')

        actual = _year_period_of(timestamp)
        expected = (
            parse('2024/01/01T00:00:00'),
            parse('2024/12/31T23:59:59.999999')
        )
        self.assertEqual(actual, expected)

    def test_get_timestamp_to_sync_1(self):
        """
        Test the `_get_timestamp_to_sync` method returns default timestamp
        when there is no reservation history for any period types.
        """
        # Assert outcome for daily period
        actual = _get_timestamp_to_sync(ReservationLog.PERIOD_DAILY)
        expected = parse('2022-01-29T00:00:00.0Z')
        self.assertEqual(actual, expected)

        # Assert outcome for monthly period
        actual = _get_timestamp_to_sync(ReservationLog.PERIOD_MONTHLY)
        expected = parse('2022-01-29T00:00:00.0Z')
        self.assertEqual(actual, expected)

        # Assert outcome for yearly period
        actual = _get_timestamp_to_sync(ReservationLog.PERIOD_YEARLY)
        expected = parse('2022-01-29T00:00:00.0Z')
        self.assertEqual(actual, expected)

    def test_get_timestamp_to_sync_2(self):
        """
        Test the `_get_timestamp_to_sync` method returns correct timestamp.
        """
        # Assert outcome for daily period
        timestamp = parse('2024/06/14T10:15:59Z')
        baker.make(
            ReservationLog,
            period_type=ReservationLog.PERIOD_DAILY,
            last_sync_at=timestamp,
            is_success=True
        )

        actual = _get_timestamp_to_sync(ReservationLog.PERIOD_DAILY)
        expected = parse('2024/06/15T00:00:00')
        self.assertEqual(actual, expected)

        # Assert outcome for monthly period
        timestamp = parse('2024/06/14T10:15:59Z')
        baker.make(
            ReservationLog,
            period_type=ReservationLog.PERIOD_MONTHLY,
            last_sync_at=timestamp,
            is_success=True
        )

        actual = _get_timestamp_to_sync(ReservationLog.PERIOD_MONTHLY)
        expected = parse('2024/06/15T00:00:00')
        self.assertEqual(actual, expected)

        # Assert outcome for yearly period
        timestamp = parse('2024/06/14T10:15:59Z')
        baker.make(
            ReservationLog,
            period_type=ReservationLog.PERIOD_YEARLY,
            last_sync_at=timestamp,
            is_success=True
        )

        actual = _get_timestamp_to_sync(ReservationLog.PERIOD_YEARLY)
        expected = parse('2024/06/15T00:00:00')
        self.assertEqual(actual, expected)

    def test_get_period_from(self):
        """
        Test the `_get_period_from` method returns correct pair of timestamps.
        """
        timestamp = parse('2024/06/14T10:15:59Z')

        # Assert outcome for daily period
        actual = _get_period_from(timestamp, ReservationLog.PERIOD_DAILY)
        expected = (
            parse('2024/06/14T00:00:00'),
            parse('2024/06/14T23:59:59.999999')
        )
        self.assertEqual(actual, expected)

        # Assert outcome for monthly period
        actual = _get_period_from(timestamp, ReservationLog.PERIOD_MONTHLY)
        expected = (
            parse('2024/06/01T00:00:00'),
            parse('2024/06/30T23:59:59.999999')
        )
        self.assertEqual(actual, expected)

        # Assert outcome for yearly period
        actual = _get_period_from(timestamp, ReservationLog.PERIOD_YEARLY)
        expected = (
            parse('2024/01/01T00:00:00'),
            parse('2024/12/31T23:59:59.999999')
        )
        self.assertEqual(actual, expected)

    @patch('app.tasks.reservations.DatasourceAPI.list')
    def test_count_events_on(self, mock_api_list):
        """
        Test the `_count_events_on` method returns correct counters.
        """
        mock_api_list.return_value = [
            {
                'id': 1,
                'hotel_id': 1,
                'room_id': '3fa85f64-5717-4562-b3fc-2c963f66afa6',
                'timestamp': '2024-06-11T14:00:00Z',
                'night_of_stay': '2024-06-12',
                'rpg_status': 1
            },
            {
                'id': 2,
                'hotel_id': 1,
                'room_id': '3fa85f64-5717-4562-b3fc-2c963f66afa7',
                'timestamp': '2024-06-11T14:00:00Z',
                'night_of_stay': '2024-06-12',
                'rpg_status': 1
            },
            {
                'id': 3,
                'hotel_id': 2,
                'room_id': '3fa85f64-5717-4562-b3fc-2c963f66afa8',
                'timestamp': '2024-06-11T14:00:00Z',
                'night_of_stay': '2024-06-12',
                'rpg_status': 1
            },
        ]

        timestamp = parse('2024/06/14T10:15:59Z')
        actual = _count_events_on(timestamp, ReservationLog.PERIOD_DAILY)
        expected = [
            {
                'hotel_id': 1,
                'total': 2,
                'period_type': ReservationLog.PERIOD_DAILY,
                'period_start': ANY,
                'period_end': ANY,
            },
            {
                'hotel_id': 2,
                'total': 1,
                'period_type': ReservationLog.PERIOD_DAILY,
                'period_start': ANY,
                'period_end': ANY,
            }
        ]
        self.assertListEqual(actual, expected)

        mock_api_list.assert_called_once_with({
            'updated__gte': '2024-06-14T00:00:00.000000Z',
            'updated__lte': '2024-06-14T23:59:59.999999Z',
            'rpg_status': 1}
        )

    @patch('app.tasks.reservations._get_period_from')
    @patch('app.tasks.reservations.DestinationAPI.upsert')
    @patch('app.tasks.reservations._count_events_on')
    @patch('app.tasks.reservations._get_timestamp_to_sync')
    def test_synchronize(
        self,
        mock_get_timestamp_to_sync,
        mock_count_events_on,
        mock_dest_api_upsert,
        mock_get_period_from
    ):
        """
        Test the `_synchronize` method performs correctly.
        """
        timestamp_to_sync = parse('2024/06/14T00:00:00Z')
        reservations = [
            {
                'hotel_id': 1,
                'total': 2,
                'period_type': ReservationLog.PERIOD_DAILY,
                'period_start': ANY,
                'period_end': ANY,
            }
        ]

        mock_get_timestamp_to_sync.return_value = timestamp_to_sync
        mock_count_events_on.return_value = reservations
        mock_get_period_from.return_value = (
            parse('2024/06/14T00:00:00Z'),
            parse('2024/06/14T23:59:59.999999Z')
        )

        _synchronize(ReservationLog.PERIOD_DAILY)

        # Assert the timestamp of last synchronization
        log = ReservationLog.objects.get()

        actual_last_sync_at = log.last_sync_at
        expected_last_sync_at = timestamp_to_sync
        self.assertEqual(actual_last_sync_at, expected_last_sync_at)

        mock_dest_api_upsert.assert_called_once_with(reservations[0])
        mock_count_events_on.assert_called_once_with(
            timestamp_to_sync,
            ReservationLog.PERIOD_DAILY
        )

    @patch('app.tasks.reservations._synchronize')
    def test_synchronize_daily_reservations(self, mock_synchronize):
        """
        Test the `synchronize_daily_reservations` task called the `_synchronize` method once.
        """
        synchronize_daily_reservations()

        mock_synchronize.assert_called_once_with(ReservationLog.PERIOD_DAILY)

    @patch('app.tasks.reservations._synchronize')
    def test_synchronize_monthly_reservations(self, mock_synchronize):
        """
        Test the `synchronize_monthly_reservations` task called the `_synchronize` method once.
        """
        synchronize_monthly_reservations()

        mock_synchronize.assert_called_once_with(ReservationLog.PERIOD_MONTHLY)

    @patch('app.tasks.reservations._synchronize')
    def test_synchronize_yearly_reservations(self, mock_synchronize):
        """
        Test the `synchronize_yearly_reservations` task called the `_synchronize` method once.
        """
        synchronize_yearly_reservations()

        mock_synchronize.assert_called_once_with(ReservationLog.PERIOD_YEARLY)
