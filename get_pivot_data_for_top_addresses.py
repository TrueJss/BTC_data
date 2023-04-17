import requests
import pandas as pd
import datetime
import pyodbc
import configparser
import time
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from bs4 import BeautifulSoup

start_time = (datetime.datetime.now())

#auth data for SQL connection
config = configparser.ConfigParser()
config.read('settings.ini')

driver = config['Settings']['driver']
server = config['Settings']['server']
database = config['Settings']['database']
uid = config['Settings']['username']
password = config['Settings']['password']

#connection string
cnxn = pyodbc.connect(
    DRIVER=driver, 
    SERVER=server, 
    DATABASE=database, 
    UID=uid, 
    PWD=password
    )
connection_string = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+uid+';PWD='+password
cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
#print(connection_string, cnxn_url)
engine = create_engine(cnxn_url)
#open cursor
cursor = cnxn.cursor()

cursor.execute("""
drop table if exists [dbo].[btc_pivot_data]
""")
cnxn.commit()

#start parsing pivot data of top richest wallets
response = requests.get("https://bitinfocharts.com/top-100-richest-bitcoin-addresses.html")
print(str(response)+' top-100 page')
time.sleep(0.2)
html = response.text
soup = BeautifulSoup(html, 'lxml')

#two tables because add split table
table1 = soup.find('table', id='tblOne')

#take pivot_headers for table
pivot_headers = []
for i in table1.find_all('th'):
    title = i.text
    pivot_headers.append(title)
    
pivot_headers[0] = 'id'

#creating DataFrame
pivot_df = pd.DataFrame(columns = pivot_headers)
pivot_df.columns = pivot_df.columns.str.replace(' ', '_').str.lower()
pivot_df = pivot_df.rename(columns = {'balance_△1w/△1m' : 'balance', '%_of_coins' : 'percent_of_coins'})
print(pivot_df)

#choosing page to parse
for page_num in range(1, 21, 1):
    if page_num == 1:
        response = requests.get("https://bitinfocharts.com/top-100-richest-bitcoin-addresses.html")
        print(page_num, response)
        time.sleep(0.2)
        html = response.text

        soup = BeautifulSoup(html, 'lxml')

        #two tables because add split table
        table1 = soup.find('table', id='tblOne')
        table2 = soup.find('table', id='tblOne2')

        # Create a for loop to fill data to pivot_df
        for j in table1.find_all('tr')[1:]:
            row_data = j.find_all('td')
            row = [i.text for i in row_data]
            length = len(pivot_df)
            pivot_df.loc[length] = row
            
        for j in table2.find_all('tr'):
            row_data = j.find_all('td')
            row = [i.text for i in row_data]
            length = len(pivot_df)
            pivot_df.loc[length] = row
    else:
        response = requests.get('https://bitinfocharts.com/top-100-richest-bitcoin-addresses-'+str(page_num)+'.html')
        print(page_num, response)
        time.sleep(0.2)
        html = response.text

        soup = BeautifulSoup(html, 'lxml')

        # two tables because advertisements is splitting the table
        table1 = soup.find('table', id='tblOne')
        table2 = soup.find('table', id='tblOne2')

        # Create a for loop to fill data to pivot_df
        for j in table1.find_all('tr')[1:]:
            row_data = j.find_all('td')
            row = [i.text for i in row_data]
            length = len(pivot_df)
            pivot_df.loc[length] = row
            
        for j in table2.find_all('tr'):
            row_data = j.find_all('td')
            row = [i.text for i in row_data]
            length = len(pivot_df)
            pivot_df.loc[length] = row

# cleaning address data    
pivot_df.address = pivot_df.address.replace({'wallet.*' : ''}, regex = True)\
    .replace({'\.\.' : ''}, regex = True)\
    .replace({'\s.*' : ''}, regex = True)
    
# pivot_df.to_csv('pivot_data.csv', sep = ';', index = False)
pivot_df.to_sql('btc_pivot_data', schema='dbo', con = engine, index = False)
result = engine.execute('select count(*) from [dbo].[btc_pivot_data]')
result.fetchall()


finish_time = (datetime.datetime.now())
print(finish_time-start_time)