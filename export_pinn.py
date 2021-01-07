from modules.get_data import db_data
from modules.send_table import send_csv
from modules.transform_data import transform

def main():
    # Get data
    ## mysql query
    query_customers = "SELECT * from ebdb.view_customers vc"
    query_orders    = "SELECT * from ebdb.view_orders vo"
    ## get data
    db_data(query_customers,'raw_database_customers')
    db_data(query_orders,'raw_database_orders')

    # transform data
    transform()

    ## send data
    send_csv()


if __name__ == "__main__":
    main()