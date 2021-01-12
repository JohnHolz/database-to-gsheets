## importing libs
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import json

def create_connection(file = 'config/access.json'):
    ## reading access .json config file
    with open(file) as json_file:
        data = json.load(json_file)

        ## parse json
        database=data['database'] 
        user=data['user']
        password=data['password']
        host=data['host']
        port=str(data['port'])
        #if need def schema
        #schema = data['schema']  

    ## Creating connections
    con_string = 'mysql+pymysql://'+user+':'+password+'@'+host+':'+port+'/'+database
    return create_engine(con_string)

def db_data(query, csv_name):
    dbConnection = create_connection()
    dbConnection.connect()
    table = pd.read_sql(query, dbConnection)
    table.to_csv('data/{}.csv'.format(csv_name),index=False)
    print(csv_name + ' created')