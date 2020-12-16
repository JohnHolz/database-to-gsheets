## importing libs
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def send_csv():
    ## default scope
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    ## reading credentials
    credentials = ServiceAccountCredentials.from_json_keyfile_name('config/client_secret.json', scope)

    ## authorizing connection 
    client = gspread.authorize(credentials)

    ## accessing google sheet
    spreadsheet = client.open('database_perpetuo_pinn')

    ## reading file
    with open('database.csv', 'r') as file_obj:
        content = file_obj.read()

    ## sending csv
    client.import_csv(spreadsheet.id, data=content)
    
    return print("enviado")