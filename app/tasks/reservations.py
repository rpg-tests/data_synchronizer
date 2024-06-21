from celery.utils.log import get_task_logger
from datetime import datetime, timedelta
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Tuple

from app.models import ReservationLog
from app.targets.apis import DatasourceAPI, DestinationAPI
from core import celery_app


logger = get_task_logger(__name__)


def _day_period_of(date: datetime) -> Tuple:
    """
    Returns a day period of the given `date`.
    """
    start = datetime.combine(date, datetime.min.time())
    end = datetime.combine(date, datetime.max.time())

    return (start, end)


def _month_period_of(date: datetime) -> Tuple:
    """
    Returns a month period of the given `date`.
    """
    start = date.replace(day=1)
    start = datetime.combine(start, datetime.min.time())

    # We use `relativedelta` to calculate the end of month
    # So it won't blindly add 31 days to the given `date`.
    end = start + relativedelta(day=31)
    end = datetime.combine(end, datetime.max.time())

    return (start, end)


def _year_period_of(date: datetime) -> Tuple:
    """
    Returns a yearly period of the given `date`.
    """
    start = date.replace(day=1, month=1)
    start = datetime.combine(start, datetime.min.time())

    # We use `relativedelta` to calculate the end of a year
    # So it won't blindly add 31 days to the given `date`.
    end = start.replace(month=12) + relativedelta(day=31)
    end = datetime.combine(end, datetime.max.time())

    return (start, end)


def _get_timestamp_to_sync(period_type: str) -> datetime:
    """
    Calculate the timestamp to sync.
    """
    # Find the last reservation that is successfully synchronized.
    last_sync = ReservationLog.objects.filter(period_type=period_type, is_success=True)\
        .order_by('-last_sync_at').first()

    # Calculate the next timestamp of the event reservation that needs to be synchronized.
    if not last_sync:
        return parse('2022-01-29T00:00:00.0Z')  # The first date from the `snapshot_data.csv`

    anchor_timestamp = datetime.combine(last_sync.last_sync_at, datetime.min.time())
    return anchor_timestamp + timedelta(days=1)


def _get_period_from(timestamp: datetime, type: str) -> Tuple:
    """
    Returns period for a given `timestamp` according to it's period `type`.
    """
    if type == ReservationLog.PERIOD_DAILY:
        return _day_period_of(timestamp)

    if type == ReservationLog.PERIOD_MONTHLY:
        return _month_period_of(timestamp)

    if type == ReservationLog.PERIOD_YEARLY:
        return _year_period_of(timestamp)

    raise ValueError(f'Period type {type} is not supported.')


def _count_events_on(timestamp: datetime, period_type: str) -> List[Dict]:
    """
    Pulls event reservations from data provider service,
    map it by the hotel identifier, and then count the total reservations
    for each hotel in a specific period.
    """
    logger.info(f'Calculate time range period from the given {timestamp}...')
    start_time, end_time = _get_period_from(timestamp, period_type)
    req_params = {
        'updated__gte': start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        'updated__lte': end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        'rpg_status': 1,  # 1 - booking
    }

    logger.info(f"Get events from {req_params['updated__gte']} until {req_params['updated__lte']}...")
    event_api = DatasourceAPI()
    events = event_api.list(req_params)

    logger.info('Mapping events by hotel id...')
    events_map = {}
    for event in events:
        hotel_id = event['hotel_id']

        hotel_events = events_map.get(hotel_id, [])
        hotel_events.append(event)

        events_map[hotel_id] = hotel_events

    logger.info('Counting events by hotel id...')
    counters = [
        {
            'hotel_id': hotel_id,
            'total': len(events),
            'period_type': period_type,
            'period_start': start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'period_end': end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        }
        for hotel_id, events in events_map.items()
    ]

    return counters


def _synchronize(period_type: str):
    """
    Synchronize total reservations for a specific period type.
    """
    logger.info('Get events from data provider service...')
    timestamp = _get_timestamp_to_sync(period_type)
    event_counters = _count_events_on(timestamp, period_type)

    logger.info('Synchronizing total reservations...')
    api = DestinationAPI()
    for counted_event in event_counters:
        api.upsert(counted_event)

    logger.info('Logs the timestamps of synchronized reservations...')
    start_time, end_time = _get_period_from(timestamp, period_type)
    ReservationLog.objects.create(
        last_sync_at=timestamp,
        period_type=period_type,
        period_start=start_time,
        period_end=end_time,
        is_success=True
    )


@celery_app.task(name='app.tasks.reservations.synchronize_daily_reservations')
def synchronize_daily_reservations():
    """
    Task to synchronize reservations in daily basis periodically.
    """
    logger.info('Synchronize daily total reservations...')
    _synchronize(ReservationLog.PERIOD_DAILY)


@celery_app.task(name='app.tasks.reservations.synchronize_monthly_reservations')
def synchronize_monthly_reservations():
    """
    Task to synchronize reservations in monthly basis periodically.
    """
    logger.info('Synchronize monthly total reservations...')
    _synchronize(ReservationLog.PERIOD_MONTHLY)


@celery_app.task(name='app.tasks.reservations.synchronize_yearly_reservations')
def synchronize_yearly_reservations():
    """
    Task to synchronize reservations in yearly basis periodically.
    """
    logger.info('Synchronize yearly total reservations...')
    _synchronize(ReservationLog.PERIOD_YEARLY)
