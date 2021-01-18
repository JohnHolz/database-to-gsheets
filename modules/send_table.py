## importing libs
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def send_csv(dataframe, file = 'data/database.csv',gsheets_table = 'database_perpetuo_pinn',client_secrets = 'config/client_secret.json'):
    ## write table
    dataframe.to_csv(file,sep=',',index=False)
    print("database.csv created: {} lines, {} rows".format(dataframe.shape[0],dataframe.shape[1]))    

    ## default scope
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    ## reading credentials
    credentials = ServiceAccountCredentials.from_json_keyfile_name(client_secrets, scope)
   
    ## authorizing connection 
    client = gspread.authorize(credentials)

    ## accessing google sheet
    spreadsheet = client.open(gsheets_table)

    ## reading file
    with open(file, 'r') as file_obj:
        content = file_obj.read()

    ## sending csv
    client.import_csv(spreadsheet.id, data=content)
    print("enviado")