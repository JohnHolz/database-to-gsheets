from modules.get_data import db_data
from modules.send_table import send_csv
import pandas as pd
import re

def main():
    # Get data
    ## mysql query
    query_customers = "SELECT * from ebdb.view_customers vc"
    query_orders    = "SELECT * from ebdb.view_orders vo"
    
    ## get data
    db_data(query_customers,'raw_database_customers')
    db_data(query_orders,'raw_database_orders')

    # transform data
    customers = pd.read_csv('data/raw_database_customers.csv')
    orders = pd.read_csv('data/raw_database_orders.csv')

    ## keep columns
    keep_columns = ['id', 'name', 'email', 'type', 'phone', 'user_id', 'created_at', 'borned_at']
    table = customers[keep_columns]
    orders = orders[['id','customer_id', 'voucher','finished_at','status']]
    orders = orders[orders.status == 'F']
    vouchers = ['FRETEGRATIS','PRIMEIRACOMPRA','QUERO15']
    pivot = orders[orders['voucher'].isin(vouchers)]
    pivot = pivot[['customer_id','voucher']]
    for i in vouchers:
        temp = pivot[pivot.voucher == i].groupby('customer_id').count().reset_index()
        temp.columns = ['customer_id',i]
        table = table.merge(temp, how='left',left_on = 'id',right_on = 'customer_id')
    ultimos_pedidos = orders[['customer_id','finished_at']].groupby('customer_id').max().reset_index()
    ultimos_pedidos.columns = ['customer_id', 'ultimo pedido']
    qtds_pedidos = orders[['customer_id','id']].groupby('customer_id').count().reset_index()
    qtds_pedidos.columns = ['customer_id','qtd de pedidos']
    table = table.merge(ultimos_pedidos, how='left',left_on = 'id',right_on = 'customer_id')
    table = table.merge(qtds_pedidos, how='left',left_on = 'id',right_on = 'customer_id')

    string = 'customer_id'
    cols = [c for c in table.columns if c.lower()[:len(string)] != string]
    table=table[cols]
    ## cleanning special characters
    table['name'] = table['name'].str.replace('[^\w\s#@/:%.,_-]', '', flags=re.UNICODE)
    table['email']= table['email'].str.lower()
    table['name'] = table['name'].str.lower()
    table['name'] = table['name'].str.title()
    table['created_at'] = pd.to_datetime(table['created_at']).dt.strftime('%d/%m/%Y')
    table['borned_at'] = pd.to_datetime(table['borned_at']).dt.strftime('%d/%m/%Y')
    table['ultimo pedido'] = pd.to_datetime(table['ultimo pedido']).dt.strftime('%d/%m/%Y')

    ## creating csv to next script
    table.to_csv('data/database.csv',sep=',',index=False)
    print("database.csv created: {} lines, {} rows".format(table.shape[0],table.shape[1]))

    ## send data
    send_csv()

if __name__ == "__main__":
    main()
