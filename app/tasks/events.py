import pandas as pd
import numpy as np

from celery.utils.log import get_task_logger
from django.conf import settings
from typing import Dict, List, Union

from app.models import EventLog
from app.targets.apis import DatasourceAPI
from core import celery_app


logger = get_task_logger(__name__)


def _load_events() -> pd.DataFrame:
    """
    Load snapshot data of the events and sort it by the date of `event_timestamp`.
    """
    # Load dataset.
    data_path = f'{settings.ROOT_DIR}/snapshot_data.csv'
    df_events = pd.read_csv(
        data_path,
        dtype={
            'room_reservation_id': str,
            'night_of_stay': str,
            'id': 'int64',
            'status': 'int64',
            'event_timestamp': str,
            'hotel_id': 'int64',
        }
    )

    # Convert `night_of_stay` and `event_timestamp` columns to datetime.
    df_events['night_of_stay'] = pd.to_datetime(df_events['night_of_stay'], format='ISO8601')
    df_events['event_timestamp'] = pd.to_datetime(df_events['event_timestamp'], format='ISO8601')

    # Sort data by `event_timestamp` in ascending order.
    df_events_sorted = df_events.sort_values('event_timestamp', ascending=True).reset_index(drop=True)
    df_events_sorted['event_date'] = df_events_sorted['event_timestamp'].dt.date

    return df_events_sorted


def _get_next_date_to_sync(df_events: pd.DataFrame) -> Union[pd.Timestamp, None]:
    """
    Return the next date of the event that needs to be synchronized (if any).
    """
    # Find the last event that is successfully synchronized.
    last_sync = EventLog.objects.filter(is_success=True)\
        .order_by('-event_date').first()

    # Find the next date of the event that needs to be synchronized.
    event_dates = df_events['event_date'].unique()

    if not last_sync:
        return event_dates[0]

    try:
        current_date_index = np.where(event_dates == last_sync.event_date)[0][0]
    except IndexError:
        raise ValueError('Last synchronized date is not found in the predefined data.')

    if current_date_index < len(event_dates) - 1:
        return event_dates[current_date_index + 1]

    # All events has been synchronized.
    return None


def _events_dataframe_to_payload_data(events_to_be_sync: pd.DataFrame) -> List[Dict]:
    """
    Convert events dataframe to the list of event dictionaries.
    """
    payload_data = []

    for _, event in events_to_be_sync.iterrows():
        payload_data.append({
            'id': event['id'],
            'hotel_id': event['hotel_id'],
            'room_id': event['room_reservation_id'],
            'timestamp': event['event_timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'rpg_status': event['status'],
            'night_of_stay': event['night_of_stay'].strftime('%Y-%m-%d'),
        })

    return payload_data


@celery_app.task(name='app.tasks.events.synchronize_events')
def synchronize_events():
    """
    Task to synchronize events periodically.
    """
    logger.info('Load events from CSV file...')
    df_events = _load_events()

    logger.info('Find events to be synchronized...')
    next_date_to_sync = _get_next_date_to_sync(df_events)

    if next_date_to_sync is None:
        logger.info('There is no new event found in the csv file. Aborting operation...')
        return

    logger.info(f'Next timestamp to be synchronized is: {next_date_to_sync}...')

    logger.info('Filtering events on the CSV file with the timestamp...')
    events_to_be_sync = df_events[(df_events['event_date'] == next_date_to_sync)]

    logger.info('Converting selected events into request data...')
    payload_data = _events_dataframe_to_payload_data(events_to_be_sync)

    logger.info('Synchronizing events...')
    api = DatasourceAPI()
    for payload in payload_data:
        api.upsert(payload)

    logger.info('Logs the timestamp of synchronized events...')
    EventLog.objects.create(event_date=next_date_to_sync, is_success=True)
