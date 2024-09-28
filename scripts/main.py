
from tool_functions.py import edit_year, edit_name, delete_nums, process_email_custom
import clickhouse_connect
import numpy as np
import pandas as pd
import re



client = clickhouse_connect.get_client(host='HOSTNAME.clickhouse.cloud', port=8123, username='default', password='')

# accessing input data
df1 = client.query('SELECT * FROM table_dataset1')
df2 = client.query('SELECT * FROM table_dataset2')
df3 = client.query('SELECT * FROM table_dataset3')


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

merged_df = pd.concat([df_1, df_2, df_3])
grouped_df = merged_df.groupby('email')['uid'].apply(list).reset_index()['uid']




print('complete!!!')




# final_df

# client.insert('table_results', final_df)
