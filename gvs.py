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
url = 'http://utg.ua/wp-content/uploads/cdd/ARCHIVE/UA/'
dir_raw = 'gvs_raw'
link_pattern = 'AllGMS'
rar_file = 'AllGMS_UTG_ua.rar'
title_str = 'utg_gvs'

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

# Функція отримання даних про видобуток газу з одного файлу (за один день)
# Вимагає завантаження розпакованих файлів Укртрансгазу (архів AllGMS_UTG_ua.rar тут: http://utg.ua/wp-content/uploads/cdd/ARCHIVE/UA/)
# у папку gvs_raw, що знаходиться одній папці зі скриптом

def get_gas_data(file):
    print(file)
    df_raw = pd.read_excel(os.path.join(dir_raw, file)).iloc[1:,1:7]
    df_raw.columns = ['number', 'station', 'capacity_m_m3', 'transit_request_m_m3', 'transit_m_m3', 'free_capacity_m_m3']
    df = df_raw.loc[pd.notnull(df_raw.number)]
    df['date'] = file
    df['date'] = df['date'].str.replace('AllGMS_UTG_ua_', '').str.replace('.xlsx', '')
    df['date'] = pd.to_datetime(df['date'], format = '%d.%m.%Y')
    df['date'] = [d.date() for d in df['date']]
    df= df.iloc[3:,:]
    df[df.columns[2:-1]] = df[df.columns[2:-1]].apply(pd.to_numeric)
    df = df.iloc[:,[-1, 0, 1, 2, 3, 4, 5]]
    return (df)

# Закачка даних всіх файлів у спільний список
files = os.listdir(dir_raw)
all_dfs_list = list(map(get_gas_data, files))

# Об'єднання в єдину таблицю
all_dfs = pd.concat([df for df in all_dfs_list]).sort_values(['date', 'number']).drop('number', axis = 1)
print(all_dfs.head(20))

# Запис файлів у форматах .csv та .xlsx

if not os.path.exists("output"):
    os.makedirs("output")

strdate = all_dfs['date'].iloc[-1].strftime("%y%m%d")        

all_dfs.to_csv(os.path.join('output', title_str+'_'+strdate+'.csv'), encoding = 'utf-8', index = False)
all_dfs.to_excel(os.path.join('output', title_str+'_'+strdate+'.xlsx'), encoding = 'utf-8', index = False)
