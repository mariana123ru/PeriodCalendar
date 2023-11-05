Period Calendar project

Usefully links
- [MD editor](https://pandao.github.io/editor.md/en.html "MD editor")
- [Quick start for Python in Google calendar](https://developers.google.com/calendar/api/quickstart/python?hl=ru "Quick start")
- [OAuth 2.0 Client IDs](https://console.cloud.google.com/apis/credentials?hl=ru&project=period-calendar-377513)

For venv activation
1) Set-ExecutionPolicy RemoteSigned -Scope Process
2) .\venv\Scripts\activate

Mode selection: `.\venv\Scripts\python.exe .\src\main.py 2 --test_mode --full_reboot`

For pythoneverywhere: `python /home/mariana123/PeriodCalendar/src/main.py 2`

If token is old
1) Delete the credentials.json and token.json
2) Go to [OAuth 2.0 Client IDs](https://console.cloud.google.com/apis/credentials?hl=ru&project=period-calendar-377513) 
3) Create new creds (OAuth 2.0, desktop version), download it and save as credentials.json 

Have fun!

