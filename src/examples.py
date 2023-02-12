from __future__ import print_function

import datetime
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def print_calendar_list() -> None:
    # If modifying these scopes, delete the file token.json.
    SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
    flow = InstalledAppFlow.from_client_secrets_file('../resources/credentials.json', SCOPES)
    creds = flow.run_local_server(port=0)
    service = build('calendar', 'v3', credentials=creds)
    page_token = None
    while True:
        calendar_list = service.calendarList().list(pageToken=page_token).execute()
        for calendar_list_entry in calendar_list['items']:
            print(f"For id - {calendar_list_entry['id']}, summary is {calendar_list_entry['summary']}")
        page_token = calendar_list.get('nextPageToken')
        if not page_token:
            break


print_calendar_list()
