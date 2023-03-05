from datetime import datetime
from datetime import timedelta

import os.path
import pandas as pd
import logging
import argparse

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/calendar']  # TODO: сделать два скоупа и два крелдса
# TODO: в режиме тестирования токен живет 7 дней, Token has been expired or revoked - перейти в прод?
CALENDAR_RED: str = 'miemagrq7j7e3rfb7q333u794g@group.calendar.google.com'
CALENDAR_RED_DAYS: str = '5142218fae4bc3ca21bb8de33ae7516fea914eb0a3d2b171816a7a1e58716ddb@group.calendar.google.com'
TOKEN_PATH = 'resources/token.json'
CREDS_PATH = 'resources/credentials.json'

logging.basicConfig(level=logging.DEBUG)


def event_extractor(calendar_id: str, service, date_from: str, drop_event_id: bool = True) -> pd.DataFrame:
    """
    Extract all events from calendar in interval
    @param calendar_id: id of the calendar, see examples.py
    @param service: google object
    @param date_from:
    @param drop_event_id:
    """
    datetime_from = datetime.strptime(date_from, '%Y-%m-%d').isoformat() + 'Z'

    events_result = service.events().list(calendarId=calendar_id, timeMin=datetime_from, singleEvents=True,
                                          orderBy='startTime').execute()
    events = events_result.get('items', [])

    df = pd.DataFrame(columns=['start', 'end', 'summary', 'event_id'], data=None)

    for event in events:
        start = event['start'].get('date', event['start'].get('date'))
        start_dt = datetime.strptime(start, '%Y-%m-%d')
        end = event['end'].get('date', event['end'].get('date'))
        end_dt = datetime.strptime(end, '%Y-%m-%d')
        row = {'start': start_dt, 'end': end_dt, 'summary': event['summary'], 'event_id': event['id']}
        df = pd.concat([df, pd.DataFrame([row])], axis=0, ignore_index=True)

    logging.info(f'event_extractor end with {df.shape[0]}')

    if drop_event_id:
        return df.drop('event_id', axis=1)
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
    # TODO: Переписать нафиг

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
    logging.info(f'period_predictions end with number_of_periods_to_predict = {number_of_periods_to_predict}')
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


def update_event_summary(event_id: str, calendar_id: str, event_summary: str, service) -> None:
    """
    Update summary of the event in calendar
    @param event_id:
    @param calendar_id:
    @param event_summary:
    @param service:
    @return:
    """
    event = service.events().get(calendarId=calendar_id, eventId=event_id).execute()
    event['summary'] = event_summary
    service.events().update(calendarId=calendar_id, eventId=event['id'], body=event).execute()
    print(f"Event {event['start'].get('date', event['start'].get('date'))} was updated with {event_summary}")


def day_of_period_calculation(df: pd.DataFrame, date_from: str) -> dict:
    """
    For each day after date_from from df calculate day of period
    """
    df = df[df['start'] >= date_from]
    day_period_dict = dict()
    for index, row in df.iterrows():
        for i in range(int(row['period'])):
            event_date = (row['start'] + timedelta(days=i)).strftime('%Y-%m-%d')
            period_day_threshold = row['duration']
            ovulation_days = int(row['period'] / 2) - 1
            if i < period_day_threshold:
                summary = f'Active, period day = {i + 1}'
            elif abs(i - ovulation_days) <= 1:
                summary = f'Ovulation, period day = {i + 1}'
            elif i >= row['period'] - 1:
                summary = f'Be ready, period day = {i + 1}'
            else:
                summary = f'Just day, period day = {i + 1}'
            if row['summary'][:10] == 'Prediction':
                summary = row['summary'] + ': ' + summary

            day_period_dict[event_date] = summary
    print(f"day_of_period_calculation end, date from = {date_from}")
    return day_period_dict


def calculate_start_date_for_recreate_events(df: pd.DataFrame, full_reboot: bool, date_from: str) -> str:
    """
    @param df: dataframe without predictions
    @param full_reboot: True - recreate all events
    @param date_from: date from
    @return: new date_from
    """
    if not full_reboot:
        df_valid_period = df[df['is_valid_period'] == 1]
        date_from = df_valid_period.take([-2]).reset_index().drop('index', axis=1)['start'][0].strftime('%Y-%m-%d')
    return date_from


def check_and_recreate_event(date_from: str, service, day_period_dict: dict, full_reboot: bool) -> None:
    """
    Check all events from the calendar CALENDAR_RED_DAYS in after date_from and recreate if new event_summary
    from the dict is not the same as old one
    """
    df_existing_events = event_extractor(calendar_id=CALENDAR_RED_DAYS, service=service, date_from=date_from,
                                         drop_event_id=False)
    df_existing_events['event_date'] = df_existing_events['start'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df_existing_events = df_existing_events.drop(['start', 'end'], axis=1)

    predicted_events_max_date = max(k for k, v in day_period_dict.items())

    if full_reboot:
        print(f"Full reboot mode")
        # Delete all existing events
        for index, existing_event in df_existing_events.iterrows():
            service.events().delete(calendarId=CALENDAR_RED_DAYS, eventId=existing_event['event_id']).execute()
            print(f"Delete event = {existing_event['summary']}, date = {existing_event['event_date']}")
        # Set existing_event_max_date so, after add all new events
        existing_event_max_date = (datetime.strptime(date_from, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        print(f"Non full reboot mode")
        for index, existing_event in df_existing_events.iterrows():
            if existing_event['event_date'] > predicted_events_max_date:
                service.events().delete(calendarId=CALENDAR_RED_DAYS, eventId=existing_event['event_id']).execute()
                print(f"Delete event = {existing_event['summary']}, date = {existing_event['event_date']}, too far "
                      f"predict")
            else:
                event_summary_new = day_period_dict[existing_event['event_date']]
                if event_summary_new != existing_event['summary']:
                    print(f"Date {existing_event['event_date']}, old event = {existing_event['summary']}, "
                          f"new - {event_summary_new}, recreate!")
                    update_event_summary(calendar_id=CALENDAR_RED_DAYS, event_id=existing_event['event_id'],
                                         service=service, event_summary=event_summary_new)
                else:
                    print(f"Date {existing_event['event_date']}, old event = {existing_event['summary']}, "
                          f"new is the same, do nothing")
        existing_event_max_date = df_existing_events['event_date'].max()
    # Add new events
    for event_date, event_summary in day_period_dict.items():
        if event_date > existing_event_max_date:
            add_event(event_date=event_date, calendar_id=CALENDAR_RED_DAYS, event_summary=event_summary,
                      service=service)
            print(f'Date {event_date}, add new event = {event_summary}')


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


def change_cwd() -> None:
    """
    Change working directory if needed
    """
    project_name = 'PeriodCalendar'

    work_dir: str = os.getcwd()
    print(f'Old working dir = {work_dir}')
    if work_dir != '/home/mariana123':
        period_index = work_dir.rfind(project_name)
        new_work_dir = work_dir[:period_index + len(project_name)]
    else:
        new_work_dir = '/home/mariana123/PeriodCalendar'  # For pythoneverywhere
    print(f'New working dir = {os.getcwd()}')
    os.chdir(new_work_dir)


def main(calendar_id: str, date_from: str):
    """
    Do all work
    """
    change_cwd()

    service = build_service()

    parser = argparse.ArgumentParser()
    parser.add_argument("--test_mode", action="store_true", help="run in test mode without change the calendar")
    parser.add_argument("--full_reboot", action="store_true", help="delete all existing events")
    parser.add_argument("number_of_periods_to_predict", type=int, help="number_of_periods_to_predict", nargs='?',
                        const=2, default=2)
    args = parser.parse_args()

    full_reboot = args.full_reboot
    number_of_periods_to_predict = args.number_of_periods_to_predict

    df_red_events = event_extractor(calendar_id=calendar_id, service=service, date_from=date_from)
    df_red_events = period_analysis(df=df_red_events)
    date_from_recreate_events: str = calculate_start_date_for_recreate_events(df=df_red_events,
                                                                              date_from=date_from,
                                                                              full_reboot=full_reboot)

    df_events_and_predictions = period_predictions(df=df_red_events,
                                                   number_of_periods_to_predict=number_of_periods_to_predict)

    day_period_dict: dict = day_of_period_calculation(df=df_events_and_predictions, date_from=date_from_recreate_events)

    if not args.test_mode:
        logging.info(f'Argument parser said the program run in a normal mode')
        check_and_recreate_event(date_from=date_from_recreate_events, service=service,
                                 day_period_dict=day_period_dict, full_reboot=full_reboot)

    else:
        logging.info(f'Argument parser said the program run in a test mode')
        print(df_events_and_predictions)


if __name__ == '__main__':
    main(calendar_id=CALENDAR_RED, date_from='2022-11-01')
