import pandas as pd
import re
from datetime import date

def correct_email(email):
    if '@' in email:
        domain = email.split('@')[1]
        if domain in zanota_emails['wrong'].tolist():
            email = email.replace(domain, zanota_emails['right'].iloc[zanota_emails[zanota_emails['wrong']==domain].index.values[0]])
        return email

zanota_ls = [["gmail. com"], ["gmail.com"],["@gmial.com"] , ["@gmail.com"],["@gmail..com"], [" @gmail.com"],["@hotmaol.com "], [" @hotmail.com"],["@gmail.com.com "], [" @gmail.com"],["@hotmal.com "], [" @hotmail.com"],["@hotmail "], [" @hotmail.com"],["@.g..com "], [" @gmail.com"],["@gmail.com. "], [" @gmail.com"],["@hotmail..com "], [" @hotmail.com"],["@outlook.com.com "], [" @outlook.com"],["@gmail.con "], [" @gmail.com"],["gmail.comr", "gmail.com"],["outlook.con", "outlook.com"],["gmail.com.br","gmail.com"],["gamai.com","gmail.com"],["mail.com","gmail.com"],["hotimail.com","hotmail.com"],["gmai.com","gmail.com"],["hotmsil.com","hotmail.com"],["gmail.com.br","gmail.com"],["oultlok.com","outlook.com"],["gmil.com","gmail.com"],["hormail.com","hotmail.com"],["gmi.comr","gmail.com"],["outook.com","outlook.com"],["gmali.com","gmail.com"],["gmali.com","gmail.com"],["hitmail.com","hotmail.com"],["gmaim.com","gmail.com"],["gmi.com","gmail.com"],["rotmail.com","hotmail.com"],["gamil.com","gmail.com"],["gmael.com","gmail.com"],["hotmil.com","hotmail.com"],["gmaul.com","gmail.com"],["outolook.com","outlook.com"],["hot.com","hotmail.com"],["gmali.com","gmail.com"],["hotmai.com","hotmail.com"],["ourlook.com","outlook.com"],["yahoo.com","yahoo.com.br"],["hotmaill.com","hotmail.com"],["gamail.com","gmail.com"],["htmai.com","hotmail.com"],["gemail.com","gmail.com"],["gmaol.com","gmail.com"],["gemael.com","gmail.com"],["gmaill.com","gmail.com"],["outlok.com","outlook.com"],["gmall.com","gmail.com"],["email.com","gmail.com"],["gma.com","gmail.com"],["gameil.com","gmail.com"],["gail.com","gmail.com"],["gamail.com","gmail.com"],["outloo.com","outlook.com"]]
zanota_emails = pd.DataFrame(zanota_ls)
zanota_emails.columns = ['wrong','right']

def transform():
    date_format = '%Y-%m-%d'

    ## reading raw data
    customers = pd.read_csv('data/raw_database_customers.csv')
    orders = pd.read_csv('data/raw_database_orders.csv')

    ## keep columns to output table
    keep_columns = ['id', 'name', 'email', 'type', 'phone', 'user_id', 'created_at']
    table = customers[keep_columns]

    ## remove others
    orders = orders[['id','customer_id', 'voucher','created_at','status']]

    ## filter just finished status
    orders = orders[orders.status == 'F']

    ## primeiros pedidos
    primeiros_pedidos = orders[['customer_id','created_at']].groupby('customer_id').min().reset_index()
    primeiros_pedidos.columns = ['customer_id', 'primeiro pedido']
    table = table.merge(primeiros_pedidos, how='left',left_on = 'id',right_on = 'customer_id')

    ## ultimos pedidos
    ultimos_pedidos = orders[['customer_id','created_at']].groupby('customer_id').max().reset_index()
    ultimos_pedidos.columns = ['customer_id', 'ultimo pedido']
    table = table.merge(ultimos_pedidos, how='left',left_on = 'id',right_on = 'customer_id')

    ## qtd de pedidos
    qtds_pedidos = orders[['customer_id','id']].groupby('customer_id').count().reset_index()
    qtds_pedidos.columns = ['customer_id','qtd de pedidos']
    table = table.merge(qtds_pedidos, how='left',left_on = 'id',right_on = 'customer_id')

    ## cleanning special characters
    table['name'] = table['name'].str.replace('[^\w\s#@/:%.,_-]', '', flags=re.UNICODE)
    table['email']= table['email'].str.lower()
    table['email']= table['email'].apply(correct_email)
    table['name'] = table['name'].str.lower()
    table['name'] = table['name'].str.title()
    table['created_at'] = pd.to_datetime(table['created_at']).dt.strftime(date_format)
    # table['borned_at'] = pd.to_datetime(table['borned_at']).dt.strftime('%d/%m/%Y')
    table['dias sem pedir'] = pd.to_datetime('today') - pd.to_datetime(table[table['ultimo pedido'].isna()!=True]['ultimo pedido'])
    table['dias sem pedir'] = table['dias sem pedir'].apply(lambda x: str(x).split('days')[0])
    table['dias sem pedir'] = table['dias sem pedir'].apply(lambda x : '' if x=="NaT" else x)
    table['primeiro pedido'] = pd.to_datetime(table['primeiro pedido']).dt.strftime(date_format)
    table['ultimo pedido'] = pd.to_datetime(table['ultimo pedido']).dt.strftime(date_format)

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

    ## remove customer_id columns  
    string = 'customer_id'
    cols = [c for c in table.columns if c.lower()[:len(string)] != string]
    table=table[cols]

    print('transformed')
    return table