from toolkit.tool_functions import (edit_year, edit_name, delete_nums, process_email_custom,
                                    find_streets, find_city,
                                    extract_regcode, phone_preprocessing,
                                    get_index, find_house_and_building, clean_address)

import clickhouse_connect
import numpy as np
import pandas as pd
import re
import time
import socket
import recordlinkage
import networkx as nx

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

df1['birthdate'] = df1['birthdate'].map(lambda x: re.split('-', x))
df1['birthdate'] = df1['birthdate'].map(lambda x: list(filter(None, x)))

df1[['year', 'month', 'date']] = pd.DataFrame(df1.birthdate.tolist(), index=df1.index)
df1.drop('birthdate', axis=1, inplace=True)

df1['year'] = df1['year'].map(lambda x: edit_year(x))

df1['login'], df1['domain'] = zip(*df1['email'].apply(process_email_custom))
df1.drop('email', axis=1, inplace=True)

df1['name'] = df1['full_name'].apply(lambda x: edit_name(x))
df1.drop('full_name', axis=1, inplace=True)

clean_address(df1)
df1.drop('address', axis=1, inplace=True)

df1[['reg_code', 'number']] = df1['phone'].apply(phone_preprocessing).tolist()
df1.drop('phone', axis=1, inplace=True)

df2['birthdate'] = df2['birthdate'].map(lambda x: re.split('-', x))
df2['birthdate'] = df2['birthdate'].map(lambda x: list(filter(None, x)))

df2[['year', 'month', 'date']] = pd.DataFrame(df2.birthdate.tolist(), index=df2.index)
df2.drop('birthdate', axis=1, inplace=True)

df2['year'] = df2['year'].map(lambda x: edit_year(x))

df2['first_name'] = df2['first_name'].apply(lambda x: edit_name(x))
df2['middle_name'] = df2['middle_name'].apply(lambda x: edit_name(x))
df2['last_name'] = df2['last_name'].apply(lambda x: edit_name(x))

df2['name'] = df2[['first_name', 'middle_name', 'last_name']].agg(' '.join, axis=1)
df2.drop(['first_name', 'middle_name', 'last_name'], axis=1, inplace=True)

clean_address(df2)
df2.drop('address', axis=1, inplace=True)

df2[['reg_code', 'number']] = df2['phone'].apply(phone_preprocessing).tolist()
df2.drop('phone', axis=1, inplace=True)

df3['name'] = df3['name'].apply(lambda x: edit_name(x))

df3['login'], df3['domain'] = zip(*df3['email'].apply(process_email_custom))

df3['birthdate'] = df3['birthdate'].map(lambda x: re.split('-', x))
df3['birthdate'] = df3['birthdate'].map(lambda x: list(filter(None, x)))

df3[['year', 'month', 'date']] = pd.DataFrame(df3.birthdate.tolist(), index=df3.index)
df3.drop('birthdate', axis=1, inplace=True)

df3['year'] = df3['year'].map(lambda x: edit_year(x))


def prepare_dataset(df, source_name):
    df = df.copy()
    df['source'] = source_name
    df['uids'] = df['uid'].apply(lambda x: [x])
    return df

df1 = prepare_dataset(df1, 'df1')
df2 = prepare_dataset(df2, 'df2')
df3 = prepare_dataset(df3, 'df3')


def deduplicate_dataset(df, block_on=['year', 'month', 'date'], method='jarowinkler', threshold=0.85):
    df = df.copy()

    indexer = recordlinkage.Index()
    for field in block_on:
        if field in df.columns:
            indexer.block(field)

    candidate_links = indexer.index(df)

    compare = recordlinkage.Compare()

    if 'name' in df.columns:
        compare.string('name', 'name', method=method, threshold=threshold, label='name')
    if 'number' in df.columns:
        compare.exact('number', 'number', label='number')
    if 'login' in df.columns:
        compare.exact('login', 'login', label='login')
    if 'domain' in df.columns:
        compare.exact('domain', 'domain', label='domain')

    features = compare.compute(candidate_links, df)

    exact_match_cols = [col for col in ['number', 'login', 'domain'] if col in features.columns]

    if exact_match_cols:
        exact_match = (features[exact_match_cols] == 1).any(axis=1)
    else:
        exact_match = pd.Series(False, index=features.index)

    if 'name' in features.columns:
        name_match = features['name'] >= threshold
    else:
        name_match = pd.Series(False, index=features.index)

    matches = features[exact_match | name_match]

    G = nx.Graph()
    G.add_nodes_from(df.index)
    G.add_edges_from(matches.index)

    components = list(nx.connected_components(G))

    deduplicated_records = []
    for comp in components:
        records = df.loc[list(comp)].copy()
        merged_record = merge_records(records)
        deduplicated_records.append(merged_record)

    deduplicated_df = pd.DataFrame(deduplicated_records)
    return deduplicated_df


def merge_records(records):
    merged = {}
    if 'sources' in records.columns:
        all_sources = records['sources'].explode().unique().tolist()
    elif 'source' in records.columns:
        all_sources = records['source'].unique().tolist()
    else:
        print("Столбцы records в merge_records:", records.columns)
        raise KeyError("Столбец 'source' или 'sources' отсутствует в DataFrame 'records'")

    all_uids = records['uids'].explode().unique().tolist()
    merged['uids'] = all_uids
    merged['sources'] = all_sources
    fields = [
        'name', 'number', 'year', 'month', 'date',
        'login', 'domain', 'address', 'sex'
    ]
    for field in fields:
        if field in records.columns:
            values = records[field].dropna().unique()
            if len(values) == 1:
                merged[field] = values[0]
            elif len(values) > 1:
                if field == 'name':
                    merged[field] = max(values, key=len)
                elif field in ['number', 'login', 'domain', 'address']:
                    merged[field] = records[field].mode().iloc[0]
                else:
                    merged[field] = values[0]
            else:
                merged[field] = np.nan
        else:
            merged[field] = np.nan
    return merged


dedup_df1 = deduplicate_dataset(df1)
dedup_df2 = deduplicate_dataset(df2)
dedup_df3 = deduplicate_dataset(df3)


df_all = pd.concat([dedup_df1, dedup_df2, dedup_df3], axis=0, ignore_index=True)

G = nx.Graph()
G.add_nodes_from(df_all.index)


def link_deduplicated_records(df, block_on=['year', 'month', 'date'], method='jarowinkler', threshold=0.85):
    indexer = recordlinkage.Index()
    for field in block_on:
        if field in df.columns:
            indexer.block(field)

    candidate_links = indexer.index(df)

    compare = recordlinkage.Compare()

    if 'name' in df.columns:
        compare.string('name', 'name', method=method, threshold=threshold, label='name')
    if 'number' in df.columns:
        compare.exact('number', 'number', label='number')
    if 'login' in df.columns:
        compare.exact('login', 'login', label='login')
    if 'domain' in df.columns:
        compare.exact('domain', 'domain', label='domain')

    features = compare.compute(candidate_links, df)

    pairs = features.index.to_frame(index=False)
    pairs.columns = ['record_id_1', 'record_id_2']

    pairs['sources_l'] = df.loc[pairs['record_id_1'], 'sources'].values
    pairs['sources_r'] = df.loc[pairs['record_id_2'], 'sources'].values

    def no_common_source(row):
        return len(set(row['sources_l']).intersection(set(row['sources_r']))) == 0

    cross_source_pairs = pairs[pairs.apply(no_common_source, axis=1)]

    valid_pairs = list(zip(cross_source_pairs['record_id_1'], cross_source_pairs['record_id_2']))
    features = features.loc[valid_pairs]

    exact_match_cols = [col for col in ['number', 'login', 'domain'] if col in features.columns]

    if exact_match_cols:
        exact_match = (features[exact_match_cols] == 1).any(axis=1)
    else:
        exact_match = pd.Series(False, index=features.index)

    if 'name' in features.columns:
        name_match = features['name'] >= threshold
    else:
        name_match = pd.Series(False, index=features.index)

    matches = features[exact_match | name_match]

    G.add_edges_from(matches.index)


link_deduplicated_records(df_all)


components = list(nx.connected_components(G))

final_records = []
for comp in components:
    records = df_all.loc[list(comp)].copy()
    merged_record = merge_records(records)
    final_records.append(merged_record)

final_df = pd.DataFrame(final_records)

id_is1_list = []
id_is2_list = []
id_is3_list = []

for idx, row in final_df.iterrows():
    ids1 = []
    ids2 = []
    ids3 = []

    for uid, source in zip(row['uids'], row['sources']):
        if source == 'df1':
            ids1.append(uid)
        elif source == 'df2':
            ids2.append(uid)
        elif source == 'df3':
            ids3.append(uid)

    id_is1_list.append(ids1)
    id_is2_list.append(ids2)
    id_is3_list.append(ids3)

results_df = pd.DataFrame({
    'id_is1': id_is1_list,
    'id_is2': id_is2_list,
    'id_is3': id_is3_list
})


table_name = 'table_results'
columns = results_df.columns.tolist()
data_tuples = [tuple(x) for x in results_df.to_records(index=False)]

client.insert(table_name, data_tuples, column_names=columns)
