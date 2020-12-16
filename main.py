from modules.from_csv_to_gs import send_csv
from modules.from_db_to_csv import create_csv

def main():
    ## make the query and wright in the current directory
    create_csv()
    ## send csv to google sheets
    send_csv()

if __name__ == "__main__":
    main()
