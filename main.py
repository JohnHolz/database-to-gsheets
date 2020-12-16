from modules.csv_to_gs import send_csv
from modules.db_to_csv import create_csv

def main():
    ## make query and write as csv in current directory
    create_csv()
    ## read csv and write on a google sheets
    send_csv()

if __name__ == "__main__":
    main()
