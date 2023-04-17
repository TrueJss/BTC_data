import requests
import pandas as pd
import datetime
import configparser
import time
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from bs4 import BeautifulSoup

start_time = (datetime.datetime.now())

config = configparser.ConfigParser()
config.read('settings.ini')

driver = config['Settings']['driver']
server = config['Settings']['server']
database = config['Settings']['database']
uid = config['Settings']['username']
password = config['Settings']['password']

#connection string
connection_string = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+uid+';PWD='+password
cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

engine = create_engine(cnxn_url)

addr = pd.read_sql('select distinct address from btc_pivot_data', con = engine)


for i, row in addr.iterrows():
    link = row['address']
    url = 'https://btc1.trezor.io/address/'+link
    # print(str(url)+' processing...')
    
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'}
    
    main_log_df = pd.DataFrame(columns = ['url', 'response', 'start_time', 'end_time'])
    pages_log_df = pd.DataFrame(columns = ['url', 'response', 'start_time', 'end_time'])
    btn_links_df = pd.DataFrame(columns=['num_page'])
    
    main_page_start_time = (datetime.datetime.now())
    response = requests.get(url, headers=headers)
    print(str(url)+' processing...' + str(response))
    time.sleep(0.200)
    main_page_end_time = (datetime.datetime.now())
    main_log_row = [str(url), str(response), str(main_page_start_time), str(main_page_end_time)]
    main_log_df.loc[len(main_log_df)] = main_log_row
    
    html = response.text
    soup = BeautifulSoup(html, 'lxml')
    
    val_df = pd.DataFrame(columns=['val'])
    dt_df = pd.DataFrame(columns=['date'])
    
    #####trying to get all pages
    try:
        pages_start_time = (datetime.datetime.now())
        nav_btns = soup.find(attrs={'class': 'paging-group mx-2'})
        for a in nav_btns.find_all('a'):
            # btn_links = a.get('href')
            btn_links = a.text #get('href')
            btn_links_df.loc[len(btn_links_df)] = btn_links
        last_page = int(btn_links_df.num_page.tail(1))
        print(last_page)
        # for i in range(1, last_page+1):
        for i in range(last_page, 1, -1):
            val_df = val_df.iloc[0:0]
            dt_df = dt_df.iloc[0:0]
            pages_log_df = pages_log_df.iloc[0:0]
            final_url = str(url)+'?page='+str(i)
            response = requests.get(final_url, headers=headers)
            print('Processing multipage ' + str(final_url) + str(response))
            time.sleep(0.200)
            if response.status_code == 200:
                # pages_log_row = [str(final_url), str(response)]
                # pages_log_df.loc[len(pages_log_df)] = pages_log_row
                html = response.text
                soup = BeautifulSoup(html, 'lxml')
                #####поступления
                for inp_txs in soup.find_all(attrs={"class": "row tx-out"}):
                    own_txs = inp_txs.find_all(attrs={"class": "col-12 tx-own"})
                    for own_txn_info in own_txs:
                        txn_amt = own_txn_info.find_all(attrs={"class": "tx-amt"})
                        for txn_amt_spans in txn_amt:
                            span = txn_amt_spans.find_all('span')
                            for inp in span:
                                val = inp.get('cc')
                                dt = inp.get('tm')
                                if val != None:
                                    length = len(val_df)
                                    val_df.loc[length] = val
                                if dt != None:
                                    length = len(dt_df)
                                    dt_df.loc[length] = dt
                                   
                #####списания
                for out_txs in soup.find_all(attrs={"class": "row tx-in"}):
                    own_txs_out = out_txs.find_all(attrs={"class": "col-12 tx-own"})
                    for txn_amt_out_spans in own_txs_out:
                        span_out = txn_amt_out_spans.find_all('span')
                        for out in span_out:
                            out_val = out.get('cc')
                            out_dt = out.get('tm')
                            if out_val != None:
                                length = len(val_df)
                                val_df.loc[length] = '-'+str(out_val)
                            if out_dt != None:
                                length = len(dt_df)
                                dt_df.loc[length] = out_dt
                
                pages_end_time = (datetime.datetime.now())
                pages_log_row = [str(final_url), str(response), str(pages_start_time), str(pages_end_time)]
                pages_log_df.loc[len(pages_log_df)] = pages_log_row
                
                df = dt_df.merge(val_df, left_index=True, right_index=True).sort_values('date')
                df['address'] = link
                df['page_num'] = i
                df.to_sql('trezor_txn_data', con = engine, index = False, if_exists = 'append')
                print('trezor_txn_data loaded')
                pages_log_df.to_sql('trezor_pages_log', con = engine, index = False, if_exists = 'append')
                print('trezor_pages_log loaded')
            
            else:
                pages_end_time = (datetime.datetime.now())
                pages_log_row = [str(final_url), str(response), str(pages_start_time), str(pages_end_time)]
                pages_log_df.loc[len(pages_log_df)] = pages_log_row
                
                pages_log_df.to_sql('trezor_pages_log', con = engine, index = False, if_exists = 'append')
                print('trezor_pages_log loaded')
    except:
        pages_start_time = (datetime.datetime.now())
        response = requests.get(url, headers=headers)
        print('Processing single_page ' + str(url) + str(response))
        time.sleep(0.200)
        if response.status_code == 200:
            # pages_log_row = [str(url), str(response)]
            # pages_log_df.loc[len(pages_log_df)] = pages_log_row
            html = response.text
            soup = BeautifulSoup(html, 'lxml')
            #####поступления
            for inp_txs in soup.find_all(attrs={"class": "row tx-out"}):
                own_txs = inp_txs.find_all(attrs={"class": "col-12 tx-own"})
                for own_txn_info in own_txs:
                    txn_amt = own_txn_info.find_all(attrs={"class": "tx-amt"})
                    for txn_amt_spans in txn_amt:
                        span = txn_amt_spans.find_all('span')
                        for inp in span:
                            val = inp.get('cc')
                            dt = inp.get('tm')
                            if val != None:
                                length = len(val_df)
                                val_df.loc[length] = val
                            if dt != None:
                                length = len(dt_df)
                                dt_df.loc[length] = dt
                               
            #####списания
            for out_txs in soup.find_all(attrs={"class": "row tx-in"}):
                own_txs_out = out_txs.find_all(attrs={"class": "col-12 tx-own"})
                for txn_amt_out_spans in own_txs_out:
                    span_out = txn_amt_out_spans.find_all('span')
                    for out in span_out:
                        out_val = out.get('cc')
                        out_dt = out.get('tm')
                        if out_val != None:
                            length = len(val_df)
                            val_df.loc[length] = '-'+str(out_val)
                        if out_dt != None:
                            length = len(dt_df)
                            dt_df.loc[length] = out_dt
            
            pages_end_time = (datetime.datetime.now())
            pages_log_row = [str(url), str(response), str(pages_start_time), str(pages_end_time)]
            pages_log_df.loc[len(pages_log_df)] = pages_log_row
            
            df = dt_df.merge(val_df, left_index=True, right_index=True).sort_values('date')
            df['address'] = link
            df['page_num'] = 1
            df.to_sql('trezor_txn_data', con = engine, index = False, if_exists = 'append')
            print('trezor_txn_data loaded')
            
            pages_log_df.to_sql('trezor_pages_log', con = engine, index = False, if_exists = 'append')
            print('trezor_pages_log loaded')
        else:
            pages_end_time = (datetime.datetime.now())
            pages_log_row = [str(url), str(response), str(pages_start_time), str(pages_end_time)]
            pages_log_df.loc[len(pages_log_df)] = pages_log_row
            
            pages_log_df.to_sql('trezor_pages_log', con = engine, index = False, if_exists = 'append')
            print('trezor_pages_log loaded')   

finish_time = (datetime.datetime.now())
print(finish_time-start_time)