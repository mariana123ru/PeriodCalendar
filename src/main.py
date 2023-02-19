# from __future__ import print_function

import datetime
import os.path
import pandas as pd
import logging

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/calendar']  # TODO: сделать два скоупа и два крелдса
CALENDAR_RED: str = 'miemagrq7j7e3rfb7q333u794g@group.calendar.google.com'
CALENDAR_RED_DAYS: str = '5142218fae4bc3ca21bb8de33ae7516fea914eb0a3d2b171816a7a1e58716ddb@group.calendar.google.com'

logging.basicConfig(level=logging.DEBUG)


def event_extractor(calendar_id: str, service, date_from: str,
                    date_to: str = datetime.datetime.today().strftime('%Y-%m-%d')) -> pd.DataFrame:
    """
    Extract all events from calendar in interval
    :param calendar_id: id of the calendar, see examples.py
    :param service: google object
    :param date_from: date from
    :param date_to: date to
    :return: dataframe
    """
    datetime_from = datetime.datetime.strptime(date_from, '%Y-%m-%d').isoformat() + 'Z'
    datetime_to = datetime.datetime.strptime(date_to, '%Y-%m-%d').isoformat() + 'Z'

    events_result = service.events().list(calendarId=calendar_id, timeMin=datetime_from, timeMax=datetime_to,
                                          singleEvents=True, orderBy='startTime').execute()
    events = events_result.get('items', [])

    df = pd.DataFrame(columns=['start', 'end', 'summary'], data=None)

    for event in events:
        start = event['start'].get('date', event['start'].get('date'))
        end = event['end'].get('date', event['end'].get('date'))
        row = {'start': start, 'end': end, 'summary': event['summary']}
        df = pd.concat([df, pd.DataFrame([row])], axis=0, ignore_index=True)

    logging.info(f'event_extractor end with {df.shape[0]}')
    return df


def period_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate some stat for period data
    """
    df.sort_values('start', inplace=True)
    df[['start', 'end']] = df[['start', 'end']].apply(pd.to_datetime)
    df['duration'] = (df['end'] - df['start']).dt.days + 1
    df['real_period'] = df['duration'].apply(lambda x: 1 if x >= 3 else 0)
    df['previous_start'] = df.groupby(['real_period'])['start'].shift(1)

    df['period'] = df.apply(lambda row:
                            (row['start'] - row['previous_start']).days
                            if row['previous_start'] == row['previous_start'] and row['real_period'] == 1
                            else None, axis=1)
    logging.info(f'period_analysis end')
    return df


def period_prediction(df: pd.DataFrame) -> pd.DataFrame:
    # TODO: добавить тестов, что не среди 3ех последних записей нет провала
    df_last_3_real_periods = df[df['real_period'] == 1].tail(3).copy()
    average_period = int(df_last_3_real_periods['period'].mean())
    average_duration = int(df_last_3_real_periods['duration'].mean())

    start_of_last_period = df_last_3_real_periods['start'].tail(1).iloc[0]
    predicted_start = start_of_last_period + datetime.timedelta(days=average_period)
    predicted_end = predicted_start + datetime.timedelta(days=average_duration)

    predicted_event = [{'start': predicted_start, 'end': predicted_end, 'summary': 'prediction',
                        'duration': average_duration, 'real_period': 1, 'previous_start': start_of_last_period,
                        'period': average_period}]

    df = pd.concat([df, pd.DataFrame(data=predicted_event)]).reset_index().drop('index', axis=1)

    logging.info(f'period_prediction end')
    return df


def add_event(event_date: str, calendar_id, event_summary: str, service) -> None:
    event = {
        'summary': event_summary,
        'start': {
            'date': event_date,
        },
        'end': {
            'date': event_date,
        }}
    event = service.events().insert(calendarId=calendar_id, body=event).execute()


def delete_events(date_from: str, date_to: str, service) -> None:
    """
    Delete all events from the calendar CALENDAR_RED_DAYS in interval
    """
    page_token = None
    while True:
        events = service.events().list(calendarId=CALENDAR_RED_DAYS, pageToken=page_token).execute()
        for event in events['items']:
            event_datetime = event['start'].get('dateTime', event['start'].get('date'))
            event_date = event_datetime[0:10]
            event_id = event['id']
            if date_to >= event_date >= date_from:
                service.events().delete(calendarId=CALENDAR_RED_DAYS, eventId=event_id).execute()
                print(f"Delete event = {event['summary']}, date = {event_date}, {event_id}")
        page_token = events.get('nextPageToken')
        if not page_token:
            break


def day_of_period_calculation(df: pd.DataFrame, service) -> None:
    df = df.tail(2)
    for index, row in df.iterrows():
        for i in range(int(row['period'])):
            event_date = (row['previous_start'] + datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            period_day_threshold = row['duration']
            ovulation_days = int(row['period'] / 2) - 1
            if i < period_day_threshold:
                summary = f'Active, period day = {i + 1}'
            elif abs(i - ovulation_days) <= 1:
                summary = f'Ovulation, period day = {i + 1}'
            else:
                summary = f'Just day, period day = {i + 1}'
            if row['summary'] == 'prediction':
                summary = 'Prediction ' + summary
            # print(f"{event_date} - {summary}")
            add_event(event_date=event_date, calendar_id=CALENDAR_RED_DAYS, event_summary=summary, service=service)
    print(f"day_of_period_calculation end")


def main(calendar_id: str, date_from: str):
    """
    Do all work
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('../resources/token.json'):
        creds = Credentials.from_authorized_user_file('../resources/token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '../resources/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('../resources/token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('calendar', 'v3', credentials=creds)
        df_raw = event_extractor(calendar_id=calendar_id, service=service, date_from=date_from)
        df = period_analysis(df=df_raw)
        df = period_prediction(df=df)
        df = period_prediction(df=df)
        day_of_period_calculation(df=df, service=service)

    except HttpError as error:
        logging.error(f'An error occurred: {error}')
    finally:
        logging.info(f'main end')


if __name__ == '__main__':
    main(calendar_id=CALENDAR_RED, date_from='2022-11-01')

# delete_events(date_from='2022-01-01', date_to='2024-01-01', service=service)
