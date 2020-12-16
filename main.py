from modules.from_csv_to_gs import send_csv
from modules.from_db_to_csv import create_csv

def main():
    create_csv()
    send_csv()

if __name__ == "__main__":
    main()
