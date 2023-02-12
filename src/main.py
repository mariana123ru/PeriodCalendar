from __future__ import print_function

import datetime
import os.path
import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']


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

    df = pd.DataFrame(columns=['start', 'end', 'summary'])
    for event in events:
        start = event['start'].get('dateTime', event['start'].get('date'))
        end = event['end'].get('dateTime', event['end'].get('date'))
        row = {'start': start, 'end': end, 'summary': event['summary']}
        df = pd.concat([df, pd.DataFrame([row])], axis=0, ignore_index=True)

    return df


def period_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate some stat for period data
    :param df:
    :return:
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
    return df.drop('previous_start', axis=1)


def main(calendar_id: str):
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
        df_raw = event_extractor(calendar_id=calendar_id, service=service, date_from='2020-01-01')
        df = period_analysis(df=df_raw)
        print(df)

    except HttpError as error:
        print('An error occurred: %s' % error)


if __name__ == '__main__':
    main(calendar_id='miemagrq7j7e3rfb7q333u794g@group.calendar.google.com')
