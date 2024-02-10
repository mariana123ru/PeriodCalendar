from datetime import datetime
from datetime import timedelta

import os.path
import logging
import csv

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/calendar']
TOKEN_PATH = 'resources/token.json'
CREDS_PATH = 'resources/credentials.json'

LIST_OF_INTERESTS = ['Shopping', 'Beauty', 'Meetings', 'PC stuff', 'Study', 'Games', 'Sport', 'Health',
                     'mariana123ru@gmail.com', 'Job search', 'Fun', 'Travel']

logging.basicConfig(level=logging.DEBUG, filename='../log.log', format='%(asctime)s %(message)s')


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


def get_all_interesting_calendars(service) -> dict:
    """
    Extract all interesting calendars and their ids
    """
    calendar_list = service.calendarList().list().execute()

    dict_calendars = {}
    key_index = 0
    for calendar_list_entry in calendar_list['items']:
        calendar_summary = calendar_list_entry['summary']
        if calendar_summary in LIST_OF_INTERESTS:
            dict_calendars[key_index] = {'summary': calendar_summary, 'calendar_id': calendar_list_entry['id']}
            key_index = key_index + 1
    logging.info(f'get_all_interesting_calendars')
    return dict_calendars


def extract_events_from_calendar(calendar_id: str, service, date_from: datetime) -> dict:
    """
    Extract all events from calendar in interval
    """
    events_result = service.events().list(calendarId=calendar_id, timeMin=date_from.isoformat() + 'Z',
                                          singleEvents=True,
                                          orderBy='startTime').execute()
    dict_events = {}
    if bool(events_result):
        events = events_result.get('items', [])

        for event in events:
            if event['start'].get('date'):
                start_dt = datetime.strptime(event['start'].get('date'), '%Y-%m-%d')
                end_dt = datetime.strptime(event['end'].get('date'), '%Y-%m-%d')
                end_dt = end_dt - timedelta(days=1)  # for some reason, google add one day in date format only
            elif event['start'].get('dateTime'):
                start_dt = datetime.strptime(event['start'].get('dateTime')[:10], '%Y-%m-%d')
                end_dt = datetime.strptime(event['end'].get('dateTime')[:10], '%Y-%m-%d')
            else:
                print(f'Time format is crazy for event_id = {event["id"]} in start = {event["start"]}')

            is_valid_period = (end_dt - start_dt).days <= 1

            if is_valid_period:
                dict_events[event['id']] = {'event_dt': start_dt, 'event_summary': event['summary']}
        logging.info(f'extract_events_from_calendar from {calendar_id}')
    else:
        logging.info(f'No data - extract_events_from_calendar from {calendar_id}')
    return dict_events


def change_cwd() -> None:
    """
    Change working directory if needed
    """
    project_name = 'PeriodCalendar'

    work_dir: str = os.getcwd()
    logging.info(f'Old working dir = {work_dir}')
    if work_dir != '/home/mariana123':
        period_index = work_dir.rfind(project_name)
        new_work_dir = work_dir[:period_index + len(project_name)]
    else:
        new_work_dir = '/home/mariana123/PeriodCalendar'  # For pythoneverywhere
    logging.info(f'New working dir = {os.getcwd()}')
    os.chdir(new_work_dir)


def extract_all_events(dict_calendars: dict, service, date_from: datetime) -> dict:
    dict_all_events = {}
    for key in dict_calendars:
        calendar_id = dict_calendars[key]['calendar_id']
        calendar_summary = dict_calendars[key]['summary']
        dict_events = extract_events_from_calendar(calendar_id=calendar_id, service=service, date_from=date_from)
        for key_event in dict_events:
            dict_events[key_event]['calendar_id'] = calendar_id
            dict_events[key_event]['calendar_summary'] = calendar_summary
        dict_all_events.update(dict_events)
    return dict_all_events


def main():
    """
    Do all work
    """
    change_cwd()
    service = build_service()
    dict_calendars = get_all_interesting_calendars(service)
    date_from: datetime = datetime.today() - timedelta(days=365)
    dict_all_events = extract_all_events(dict_calendars=dict_calendars, service=service, date_from=date_from)

    fields = ['event_dt', 'event_summary', 'calendar_id', 'calendar_summary']
    with open('test_output2.csv', 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fields)
        w.writeheader()
        for k in dict_all_events:
            w.writerow({field: dict_all_events[k].get(field) or k for field in fields})


if __name__ == '__main__':
    main()
