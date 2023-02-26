from datetime import datetime
from datetime import timedelta

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
#TODO: в режиме тестирования токен живет 7 дней, Token has been expired or revoked - перейти в прод?
CALENDAR_RED: str = 'miemagrq7j7e3rfb7q333u794g@group.calendar.google.com'
CALENDAR_RED_DAYS: str = '5142218fae4bc3ca21bb8de33ae7516fea914eb0a3d2b171816a7a1e58716ddb@group.calendar.google.com'
TOKEN_PATH = '../resources/token.json'  # TOKEN_PATH = 'resources/token.json'
CREDS_PATH = '../resources/credentials.json'  # CREDS_PATH = 'resources/credentials.json'

logging.basicConfig(level=logging.DEBUG)


def event_extractor(calendar_id: str, service, date_from: str) -> pd.DataFrame:
    """
    Extract all events from calendar in interval
    :param calendar_id: id of the calendar, see examples.py
    :param service: google object
    :param date_from: date from
    :param date_to: date to
    :return: dataframe
    """
    datetime_to = (datetime.today() + timedelta(days=14)).isoformat() + 'Z'
    datetime_from = datetime.strptime(date_from, '%Y-%m-%d').isoformat() + 'Z'

    events_result = service.events().list(calendarId=calendar_id, timeMin=datetime_from, timeMax=datetime_to,
                                          singleEvents=True, orderBy='startTime').execute()
    events = events_result.get('items', [])

    df = pd.DataFrame(columns=['start', 'end', 'summary'], data=None)

    for event in events:
        start = event['start'].get('date', event['start'].get('date'))
        start_dt = datetime.strptime(start, '%Y-%m-%d')
        end = event['end'].get('date', event['end'].get('date'))
        end_dt = datetime.strptime(end, '%Y-%m-%d')
        row = {'start': start_dt, 'end': end_dt, 'summary': event['summary']}
        df = pd.concat([df, pd.DataFrame([row])], axis=0, ignore_index=True)

    logging.info(f'event_extractor end with {df.shape[0]}')
    return df


def period_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate some stat for period data
    """
    df.sort_values('start', inplace=True)
    df['duration'] = (df['end'] - df['start']).dt.days + 1
    df['is_valid_period'] = df['duration'].apply(lambda x: 1 if x >= 3 else 0)
    df['next_start'] = df.groupby(['is_valid_period'])['start'].shift(-1)

    df['period'] = df.apply(lambda row:
                            int((row['next_start'] - row['start']).days)
                            if row['next_start'] == row['next_start'] and row['is_valid_period'] == 1
                            else -1, axis=1)

    logging.info(f'period_analysis end')
    return df


def period_predictions(df: pd.DataFrame, number_of_periods_to_predict: int = 2) -> pd.DataFrame:
    # TODO: добавить тестов, что не среди 4ех последних записей нет провала
    df_last_4_valid_periods = df[df['is_valid_period'] == 1].tail(4).copy()
    df_last_3_full_valid_periods = df_last_4_valid_periods.head(3).copy()
    average_period = int(df_last_3_full_valid_periods['period'].mean())
    average_duration = int(df_last_3_full_valid_periods['duration'].mean())

    df_last_4_valid_periods.iloc[3, df_last_4_valid_periods.columns.get_loc('period')] = average_period
    df_last_4_valid_periods.iloc[3, df_last_4_valid_periods.columns.get_loc('next_start')] = \
        df_last_4_valid_periods.iloc[3]['start'] + timedelta(days=average_period)

    df_last_row = df_last_4_valid_periods.tail(1).copy()
    df_res = df_last_4_valid_periods.copy()

    for i in range(number_of_periods_to_predict):
        df_last_row.iloc[0, df_last_row.columns.get_loc('start')] = df_last_row.iloc[
            0, df_last_row.columns.get_loc('next_start')]
        df_last_row.iloc[0, df_last_row.columns.get_loc('end')] = df_last_row.iloc[
                                                                      0, df_last_row.columns.get_loc(
                                                                          'start')] + timedelta(
            days=average_duration)
        df_last_row.iloc[0, df_last_row.columns.get_loc('summary')] = f'Prediction {i + 1}'
        df_last_row.iloc[0, df_last_row.columns.get_loc('duration')] = average_duration
        df_last_row.iloc[0, df_last_row.columns.get_loc('is_valid_period')] = 1
        df_last_row.iloc[0, df_last_row.columns.get_loc('next_start')] = df_last_row.iloc[
                                                                             0, df_last_row.columns.get_loc(
                                                                                 'start')] + timedelta(
            days=average_period)
        df_last_row.iloc[0, df_last_row.columns.get_loc('period')] = average_period
        df_res = pd.concat([df_res, df_last_row])
    logging.info(f'period_predictions end')
    return df_res.reset_index().drop('index', axis=1)


def add_event(event_date: str, calendar_id, event_summary: str, service) -> None:
    """
    Add event to the calendar. It's important for one-day event to set up the end date on a different date
    """
    event_date_plus_one = (datetime.strptime(event_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    event = {
        'summary': event_summary,
        'start': {
            'date': event_date,
        },
        'end': {
            'date': event_date_plus_one,
        }}
    service.events().insert(calendarId=calendar_id, body=event).execute()


def delete_events(date_from: datetime, service) -> None:
    """
    Delete all events from the calendar CALENDAR_RED_DAYS in after date_from
    """
    page_token = None
    while True:
        events = service.events().list(calendarId=CALENDAR_RED_DAYS, pageToken=page_token).execute()
        for event in events['items']:
            if len(event) > 0:
                event_datetime = event['start'].get('dateTime', event['start'].get('date'))
                event_date = datetime.strptime(event_datetime[0:10], '%Y-%m-%d')
                event_id = event['id']
                if event_date >= date_from:
                    service.events().delete(calendarId=CALENDAR_RED_DAYS, eventId=event_id).execute()
                    print(f"Delete event = {event['summary']}, date = {event_date}")
            else:
                break
        page_token = events.get('nextPageToken')
        if not page_token:
            break


def day_of_period_calculation(df: pd.DataFrame, date_from: datetime, service) -> None:
    """
    For each day after date_from from df calculate day of period
    """
    df = df[df['start'] >= date_from]
    for index, row in df.iterrows():
        for i in range(int(row['period'])):
            event_date = (row['start'] + timedelta(days=i)).strftime('%Y-%m-%d')
            period_day_threshold = row['duration']
            ovulation_days = int(row['period'] / 2) - 1
            if i < period_day_threshold:
                summary = f'Active, period day = {i + 1}'
            elif abs(i - ovulation_days) <= 1:
                summary = f'Ovulation, period day = {i + 1}'
            else:
                summary = f'Just day, period day = {i + 1}'
            if row['summary'][:10] == 'Prediction':
                summary = row['summary'] + ': ' + summary
            print(f"{event_date} - {summary}")
            add_event(event_date=event_date, calendar_id=CALENDAR_RED_DAYS, event_summary=summary, service=service)
    print(f"day_of_period_calculation end, date from = {date_from}")


def delete_predictions_after_period_input(df: pd.DataFrame, service, full_reboot: bool, date_from: datetime) -> datetime:
    """
    Delete events from the last real start date
    """
    # Normal mode - remove all events after start date of second last real start date
    if not full_reboot:
        date_from = df.take([-2]).reset_index().drop('index', axis=1)['start'][0]
    # In full reboot mode remove all events from the date_from
    delete_events(date_from=date_from, service=service)
    print(f"delete_predictions_after_period_input end, date from = {date_from}")
    return date_from

def build_service():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
    try:
        service = build('calendar', 'v3', credentials=creds)
    except HttpError as error:
        logging.error(f'An error occurred: {error}')
    finally:
        logging.info(f'authorization_by_token end')
        return service


def main(calendar_id: str, date_from: str, full_reboot: bool = False):
    """
    Do all work
    """
    service = build_service()

    df_red_events = event_extractor(calendar_id=calendar_id, service=service, date_from=date_from)
    df_red_events = period_analysis(df=df_red_events)
    df_events_and_preds = period_predictions(df=df_red_events, number_of_periods_to_predict=2)

    date_for_calculation = delete_predictions_after_period_input(df=df_red_events, service=service,
                                                                 full_reboot=full_reboot, date_from=date_from)

    day_of_period_calculation(df=df_events_and_preds, date_from=date_for_calculation, service=service)


if __name__ == '__main__':
    main(calendar_id=CALENDAR_RED, date_from='2022-11-01')
    #delete_events(date_from=datetime(2022, 11, 1), service=service)
