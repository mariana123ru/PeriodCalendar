from datetime import datetime
from datetime import timedelta

import os.path
import logging
import argparse

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/calendar']
# В режиме тестирования токен живет 7 дней, Token has been expired or revoked - перешла в прод
# https://support.google.com/cloud/answer/10311615#zippy=%2Ctesting
CALENDAR_RED: str = 'miemagrq7j7e3rfb7q333u794g@group.calendar.google.com'
CALENDAR_RED_DAYS: str = '5142218fae4bc3ca21bb8de33ae7516fea914eb0a3d2b171816a7a1e58716ddb@group.calendar.google.com'
TOKEN_PATH = 'resources/token.json'
CREDS_PATH = 'resources/credentials.json'

logging.basicConfig(level=logging.DEBUG)


def event_extractor(calendar_id: str, service, date_from: datetime) -> dict:
    """
    Extract all events from calendar in interval
    @param calendar_id: id of the calendar, see examples.py
    @param service: google object
    @param date_from:
    """
    datetime_from = date_from.isoformat() + 'Z'

    events_result = service.events().list(calendarId=calendar_id, timeMin=datetime_from, singleEvents=True,
                                          orderBy='startTime').execute()
    events = events_result.get('items', [])

    dict_events = {}
    key_index = 0

    for event in events:
        start_dt = datetime.strptime(event['start'].get('date', event['start'].get('date')), '%Y-%m-%d')
        end_dt = datetime.strptime(event['end'].get('date', event['end'].get('date')), '%Y-%m-%d')
        is_valid_period = (end_dt - start_dt).days >= 3
        # From calendar CALENDAR_RED extract only valid period, for CALENDAR_RED_DAYS - all days data
        if is_valid_period or calendar_id == CALENDAR_RED_DAYS:
            dict_events[key_index] = {'start': start_dt, 'end': end_dt, 'summary': event['summary'],
                                      'event_id': event['id']}
            key_index = key_index + 1

    logging.info(f'event_extractor end with {key_index} events extracted')

    return dict_events


def period_analysis(dct: dict) -> dict:
    """
    Calculate some stat for period data
    """
    for key in dct:
        dct[key]['duration'] = (dct[key]['end'] - dct[key]['start']).days + 1
        if key + 1 in dct:
            dct[key]['period'] = (dct[key + 1]['start'] - dct[key]['start']).days

    logging.info(f'period_analysis end')
    return dct


def period_predictions(dct: dict, number_of_periods_to_predict: int = 2) -> dict:
    """
    Predict periods for number_of_periods_to_predict
    """
    number_of_full_periods = len(dct) - 1
    last_3_full_periods_keys = range(number_of_full_periods - 3, number_of_full_periods)
    average_period = int(sum(dct[key]['period'] for key in last_3_full_periods_keys) / 3)
    average_duration = int(sum(dct[key]['duration'] for key in last_3_full_periods_keys) / 3)

    # Use average period as period length for the last known period, which is not full
    dct[number_of_full_periods]['period'] = average_period

    # Make prediction
    for i in range(number_of_periods_to_predict):
        dct[number_of_full_periods + i + 1] = {}
        dct[number_of_full_periods + i + 1]['start'] = \
            dct[number_of_full_periods + i]['start'] + timedelta(days=average_period)
        dct[number_of_full_periods + i + 1]['end'] = \
            dct[number_of_full_periods + i + 1]['start'] + timedelta(days=average_duration)
        dct[number_of_full_periods + i + 1]['summary'] = f'Prediction {i + 1}'
        dct[number_of_full_periods + i + 1]['duration'] = average_duration
        dct[number_of_full_periods + i + 1]['period'] = average_period

    return dct


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


def day_of_period_calculation(dct: dict, date_recreate: datetime) -> dict:
    """
    For each day after date_from from df calculate day of period
    """
    day_period_dict = dict()
    for key in dct:
        if dct[key]['start'] >= date_recreate:
            for i in range(dct[key]['period']):
                event_date = (dct[key]['start'] + timedelta(days=i)).strftime('%Y-%m-%d')
                period_day_threshold = dct[key]['duration']
                ovulation_days = int(dct[key]['period'] / 2) - 1
                if i < period_day_threshold:
                    summary = f'Active, period day = {i + 1}'
                elif abs(i - ovulation_days) <= 1:
                    summary = f'Ovulation, period day = {i + 1}'
                elif i >= dct[key]['period'] - 1:
                    summary = f'Be ready, period day = {i + 1}'
                else:
                    summary = f'Just day, period day = {i + 1}'
                if dct[key]['summary'][:10] == 'Prediction':
                    summary = dct[key]['summary'] + ': ' + summary

                day_period_dict[event_date] = summary
    print(f"day_of_period_calculation end, recreate from = {date_recreate}")
    return day_period_dict


def calculate_recreate_date(dct: dict, full_reboot: bool, date_from: datetime) -> datetime:
    """
    @param dct: dictionary without predictions
    @param full_reboot: True - recreate all events
    @param date_from: date from
    @return: new date_from in datetime
    """
    if not full_reboot:
        recreate_dt = dct[len(dct) - 2]['start']
    else:
        recreate_dt = date_from
    return recreate_dt


def check_and_recreate_event(date_from: datetime, service, day_period_dict: dict, full_reboot: bool) -> None:
    """
    Check all events from the calendar CALENDAR_RED_DAYS in after date_from and recreate if new event_summary
    from the dict is not the same as old one
    """
    dct_exist_events = event_extractor(calendar_id=CALENDAR_RED_DAYS, service=service, date_from=date_from)

    for key in dct_exist_events:
        dct_exist_events[key]['event_date'] = dct_exist_events[key]['start'].strftime('%Y-%m-%d')

    predicted_events_max_date = max(k for k, v in day_period_dict.items())

    if full_reboot:
        print(f"Full reboot mode")
        # Delete all existing events
        for key in dct_exist_events:
            service.events().delete(calendarId=CALENDAR_RED_DAYS,
                                    eventId=dct_exist_events[key]['event_id']).execute()
            print(f"Delete event = {dct_exist_events[key]['summary']}, date = {dct_exist_events[key]['event_date']}")
        # Set existing_event_max_date so, after add all new events
        existing_event_max_date = (date_from - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        print(f"Non full reboot mode")
        for key in dct_exist_events:
            if dct_exist_events[key]['event_date'] > predicted_events_max_date:
                service.events().delete(calendarId=CALENDAR_RED_DAYS,
                                        eventId=dct_exist_events[key]['event_id']).execute()
                print(f"Delete event = {dct_exist_events[key]['summary']}, "
                      f"date = {dct_exist_events[key]['event_date']}, too far to predict")
            else:
                event_summary_new = day_period_dict[dct_exist_events[key]['event_date']]
                if event_summary_new != dct_exist_events[key]['summary']:
                    print(f"Date {dct_exist_events[key]['event_date']}, "
                          f"old event = {dct_exist_events[key]['summary']}, new - {event_summary_new}, recreate!")
                    update_event_summary(calendar_id=CALENDAR_RED_DAYS, event_id=dct_exist_events[key]['event_id'],
                                         service=service, event_summary=event_summary_new)
                else:
                    print(f"Date {dct_exist_events[key]['event_date']}, "
                          f"old event = {dct_exist_events[key]['summary']}, new is the same, do nothing")

        existing_event_max_date = max(dct_exist_events[key]['event_date'] for key in dct_exist_events)
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


def main():
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

    # Start analysis from 210 days before today, but not before '2022-11-01'
    date_from: datetime = max(datetime.today() - timedelta(days=210), datetime(2022, 11, 1))

    dct_red_events = event_extractor(calendar_id=CALENDAR_RED, service=service, date_from=date_from)
    dct_red_events = period_analysis(dct=dct_red_events)
    date_from_recreate_events: datetime = calculate_recreate_date(dct=dct_red_events,
                                                                  date_from=date_from,
                                                                  full_reboot=full_reboot)

    dct_events_and_predictions = period_predictions(dct=dct_red_events,
                                                    number_of_periods_to_predict=number_of_periods_to_predict)

    dct_day_period: dict = day_of_period_calculation(dct=dct_events_and_predictions,
                                                     date_recreate=date_from_recreate_events)

    if not args.test_mode:
        logging.info(f'Argument parser said the program run in a normal mode')
        current_date = datetime.today().strftime('%Y-%m-%d')
        current_date_day_in_period = int(dct_day_period[current_date].split('= ')[1])
        if not 6 <= current_date_day_in_period <= 20 or full_reboot:
            logging.info(f'Today is {current_date_day_in_period} day in period, full reboot is {full_reboot}, so run!')
            check_and_recreate_event(date_from=date_from_recreate_events, service=service,
                                     day_period_dict=dct_day_period, full_reboot=full_reboot)
        else:
            logging.info(f'Today is {current_date_day_in_period} day in period, so just relax!')

    else:
        logging.info(f'Argument parser said the program run in a test mode')


if __name__ == '__main__':
    main()
