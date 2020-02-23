#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import requests
from rarfile import RarFile
from urllib.request import urlretrieve
from bs4 import BeautifulSoup
from time import sleep
pd.options.mode.chained_assignment = None

# constants
url = 'https://tsoua.com/wp-content/uploads/data/cdd/ARCHIVE/UGS/EN/'
dir_raw = 'psg_raw_en'
link_pattern = 'AllUGS_'
rar_file = 'AllUGS_UTG_en-2019.rar'
rar_file2 = 'AllUGS_UTG_en.rar'
title_str = 'psg_en'

# Download files
content = BeautifulSoup(requests.get(url).text)
files = [link.get('href') for link in content.find_all('a') if link_pattern in link.get('href')]
if not os.path.exists(dir_raw):
    os.makedirs(dir_raw)
for file in files:
    urlretrieve(url + file, os.path.join(dir_raw, file))
    print('file ' + file + ' retrieved')
    sleep(1)

# unrar
rar_content_names = RarFile(os.path.join(dir_raw, rar_file)).namelist()
RarFile(os.path.join(dir_raw, rar_file)).extractall(dir_raw)
os.remove(os.path.join(dir_raw, rar_file))
rar_content_names = RarFile(os.path.join(dir_raw, rar_file2)).namelist()
RarFile(os.path.join(dir_raw, rar_file2)).extractall(dir_raw)
os.remove(os.path.join(dir_raw, rar_file2))

# Функція отримання даних про видобуток газу з одного файлу (за один день)

def get_gas_data(file):
    print(file)
    df_raw = pd.read_excel(os.path.join(dir_raw, file)).iloc[1:,1:7]
    df_raw.columns = ['object', 'volume_total_m_m3', 'in_m_m3', 'out_m_m3', 'project_capacity_m_m3', 'free_capacity_m_m3']
    index_start = [i for i, x in enumerate(df_raw.object==2) if x][0]+2
    df = df_raw.loc[index_start:]
    df['date'] = file
    df['date'] = df['date'].str.replace('AllUGS_UTG_en_', '').str.replace('.xlsx', '')
    df['date'] = pd.to_datetime(df['date'], format = '%d.%m.%Y')
    df['date'] = [d.date() for d in df['date']]
    df[df.columns[1:-1]] = df[df.columns[1:-1]].apply(pd.to_numeric)
    df = df.iloc[:,[-1, 0, 1, 2, 3, 4, 5]]
    df_total = df[df['object'].str.contains('UMMARY|ummary')].drop('object', axis = 1)
    df_comp = df[~df['object'].str.contains('UMMARY|ummary')]
    return (df_total, df_comp)

# joining files into a list
files = os.listdir(dir_raw)
all_dfs_list = list(map(get_gas_data, files))

# creating and processing full datasets: daily data and totals
all_dfs_total = pd.concat([df[0] for df in all_dfs_list]).sort_values(['date'])
all_dfs_comp = pd.concat([df[1] for df in all_dfs_list])
order = ['Bilche-Volytsko-Uherske', 'Uherske (XIV-XV)', 'Oparske', 'Dashavske', 'Bohorodchanske', 'Kehychivske', 'Verhunske', 'Krasnopopivske', 'Proletarske', 'Solokhivske', 'Chervonopartyzanske', 'Olyshivske']
all_dfs_comp['object'] = all_dfs_comp['object'].astype('category')
all_dfs_comp['object'].cat.reorder_categories(order, inplace = True)
all_dfs_comp = all_dfs_comp.sort_values(['date', 'object'])

# Запис файлів у форматах .csv та .xlsx

if not os.path.exists("output"):
    os.makedirs("output")

strdate = all_dfs_total['date'].iloc[-1].strftime("%y%m%d")        

all_dfs_total.to_csv(os.path.join('output', 'utg_'+title_str+'_totals_'+strdate+'.csv'), encoding = 'utf-8', index = False)
all_dfs_total.to_excel(os.path.join('output', 'utg_'+title_str+'_totals_'+strdate+'.xlsx'), encoding = 'utf-8', index = False)
all_dfs_comp.to_csv(os.path.join('output', 'utg_'+title_str+'_daily_'+strdate+'.csv'), encoding = 'utf-8', index = False)
all_dfs_comp.to_excel(os.path.join('output', 'utg_'+title_str+'_daily_'+strdate+'.xlsx'), encoding = 'utf-8', index = False)

# Перевірка збігу щоденних сум з файлу за компаніями та щоденних сум, вказаних у таблицях Укртрансгазу (запис у файл  Excel)

grouped_sums = all_dfs_comp.groupby('date')['volume_total_m_m3'].agg('sum').round(3).to_frame(name = 'grouped_sum').reset_index()
compare_sums = pd.merge(grouped_sums, all_dfs_total, on = 'date', how = 'outer')
compare_sums = compare_sums[['date', 'grouped_sum', 'volume_total_m_m3']]
compare_sums.to_excel(os.path.join('output', 'utg_'+title_str+'_compare_sums.xlsx'), encoding = 'utf-8', index = False)
