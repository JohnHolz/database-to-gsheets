## importing libs
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import re
import json

def create_csv():
    ## reading access .json config file
    with open('config/access.json') as json_file:
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
    dbConnection = create_engine(con_string)
    dbConnection.connect()

    ## mysql query
    query = """
    SELECT vc.name, vc.email, vc.`type`, vc.user_id, vc.created_at, vc.borned_at, COUNT(vo.finished_at) as "pedidos_finalizados", DATE_FORMAT(max(vo.finished_at), '%Y-%m-%d') as "ultimo_pedido"
    FROM ebdb.view_customers vc 
    Left join ebdb.view_orders vo on vo.customer_email = vc.email 
    GROUP by vc.email
    """

    ## reading table from mysql
    table = pd.read_sql(query, dbConnection)

    ## cleanning special characters
    table['name'] = table['name'].str.replace('[^\w\s#@/:%.,_-]', '', flags=re.UNICODE)
    table['email'] = table['email'].str.lower()
    table['name'] = table['name'].str.lower()
    table['name'] = table['name'].str.title()
    table['created_at'] = pd.to_datetime(table['created_at']).dt.strftime('%d/%m/%Y')
    table['borned_at'] = pd.to_datetime(table['borned_at']).dt.strftime('%d/%m/%Y')
    table['ultimo_pedido'] = pd.to_datetime(table['ultimo_pedido']).dt.strftime('%d/%m/%Y')
    
    ## creating csv to next script
    table.to_csv('database.csv',sep=',',index=False)
    
    return print("database.csv created: {} lines, {} rows".format(table.shape[0],table.shape[1]))
