
from toolkit.tool_functions import (edit_year, edit_name, delete_nums, process_email_custom, 
                               find_streets, find_city,
                               extract_regcode, phone_preprocessing, 
                               get_index, find_house_and_building, clean_adress)

import clickhouse_connect
import numpy as np
import pandas as pd
import re
import time
import socket


def wait_for_clickhouse(host, port, timeout=60):
    start_time = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=5):
                print("ClickHouse доступен")
                return True
        except OSError:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Не удалось подключиться к ClickHouse на {host}:{port} за {timeout} секунд.")
            time.sleep(1)

wait_for_clickhouse('localhost', 8123)

client = clickhouse_connect.get_client(host='localhost', port=8123, username='default')

# accessing input data
ds1 = client.query('SELECT * FROM table_dataset1')
rows1 = ds1.result_rows
cols1 = ds1.column_names
df1 = pd.DataFrame(rows1, columns=cols1)

ds2 = client.query('SELECT * FROM table_dataset2')
rows2 = ds2.result_rows
cols2 = ds2.column_names
df2 = pd.DataFrame(rows2, columns=cols2)


ds3 = client.query('SELECT * FROM table_dataset3')
rows3 = ds3.result_rows
cols3 = ds3.column_names
df3 = pd.DataFrame(rows3, columns=cols3)



# df1 preprocessing
#   preprocess birthdate
df1.birthdate = df1.birthdate.map(lambda x: re.split('-', x))
df1.birthdate = df1.birthdate.map(lambda x: list(filter(None, x)))

df1[['year','month', 'date']] = pd.DataFrame(df1.birthdate.tolist())
df1.drop('birthdate', axis=1, inplace=True)

df1.year = df1.year.map(lambda x: edit_year(x))

#   preprocess emails
df1['login'], df1['domain'] = zip(*df1['email'].apply(process_email_custom))
df1.drop('email', axis=1, inplace=True)

df1['name'] = df1.full_name.apply(lambda x: edit_name(x))
df1.drop('full_name', axis=1, inplace=True)


# df2 preprocessing
#   preprocess birthdate
df2.birthdate = df2.birthdate.map(lambda x: re.split('-', x))
df2.birthdate = df2.birthdate.map(lambda x: list(filter(None, x)))

df2[['year','month', 'date']] = pd.DataFrame(df2.birthdate.tolist())
df2.drop('birthdate', axis=1, inplace=True)

df2.year = df2.year.map(lambda x: edit_year(x))

#   preprocess names
df2.first_name = df2.first_name.apply(lambda x: edit_name(x))
df2.middle_name = df2.middle_name.apply(lambda x: edit_name(x))
df2.last_name = df2.last_name.apply(lambda x: edit_name(x))

df2['name'] = df2[['first_name', 'middle_name', 'last_name']].agg(' '.join, axis=1) 
df2.drop(['first_name', 'middle_name', 'last_name'], axis=1, inplace=True)

#   preprocess phones
df2.phone = df2.phone.apply(lambda x: delete_nums(x))


# df3 preprocessing
#   preprocess names
df3.name = df2.name.apply(lambda x: edit_name(x))

#   preprocess emails
df3['login'], df3['domain'] = zip(*df3['email'].apply(process_email_custom))

#   preprocess birthdate
df3.birthdate = df3.birthdate.map(lambda x: re.split('-', x))
df3.birthdate = df3.birthdate.map(lambda x: list(filter(None, x)))

df3[['year','month', 'date']] = pd.DataFrame(df3.birthdate.tolist())
df3.drop('birthdate', axis=1, inplace=True)

df3.year = df3.year.map(lambda x: edit_year(x))


df1['match_col'] = df1.name.apply(lambda x: x[:6])
df2['match_col'] = df2.name.apply(lambda x: x[:6])
df3['match_col'] = df3.name.apply(lambda x: x[:6])


final_df = pd.DataFrame()
merged = pd.merge(df2, df3, on='match_col')

final_df['id_is1'] = merged.uid_x
final_df['id_is2'] = merged.uid_x
final_df['id_is3'] = merged.uid_y

final_df['id_is1'] = final_df['id_is1'].apply(lambda x: [x])
final_df['id_is2'] = final_df['id_is2'].apply(lambda x: [x])
final_df['id_is3'] = final_df['id_is3'].apply(lambda x: [x])


data_tuples = [tuple(x) for x in final_df.to_records(index=False)]
table_name = 'table_results'
columns = final_df.columns.tolist()

client.insert(table_name, data_tuples, column_names=columns)
