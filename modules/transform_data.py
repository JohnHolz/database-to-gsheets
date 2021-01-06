import pandas as pd
import re

def transform():
    ## reading raw data
    customers = pd.read_csv('data/raw_database_customers.csv')
    orders = pd.read_csv('data/raw_database_orders.csv')

    ## keep columns to output table
    keep_columns = ['id', 'name', 'email', 'type', 'phone', 'user_id', 'created_at', 'borned_at']
    table = customers[keep_columns]

    ## remove others
    orders = orders[['id','customer_id', 'voucher','finished_at','status']]

    ## filter just finished status
    orders = orders[orders.status == 'F']

    ## adding vouchers columns
    vouchers = ['FRETEGRATIS','PRIMEIRACOMPRA','QUERO15']

    ## filter table with vouchers
    pivot = orders[orders['voucher'].isin(vouchers)]

    ## keep just this columns
    pivot = pivot[['customer_id','voucher']]
    ## add the vouchers columns 1 by 1
    for i in vouchers:
        temp = pivot[pivot.voucher == i].groupby('customer_id').count().reset_index()
        temp.columns = ['customer_id',i]
        table = table.merge(temp, how='left',left_on = 'id',right_on = 'customer_id')

    ## ultimos pedidos
    ultimos_pedidos = orders[['customer_id','finished_at']].groupby('customer_id').max().reset_index()
    ultimos_pedidos.columns = ['customer_id', 'ultimo pedido']
    table = table.merge(ultimos_pedidos, how='left',left_on = 'id',right_on = 'customer_id')

    ## qtd de pedidos
    qtds_pedidos = orders[['customer_id','id']].groupby('customer_id').count().reset_index()
    qtds_pedidos.columns = ['customer_id','qtd de pedidos']
    table = table.merge(qtds_pedidos, how='left',left_on = 'id',right_on = 'customer_id')

    ## remove customer_id columns  
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
