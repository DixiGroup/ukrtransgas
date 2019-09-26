#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import re
import requests
from functools import reduce
from operator import add
from rarfile import RarFile
from urllib.request import urlretrieve
from bs4 import BeautifulSoup
from time import sleep
pd.options.mode.chained_assignment = None

url = 'http://utg.ua/wp-content/uploads/cdd/ARCHIVE/INFLOW_EU/EN/'
dir_raw = 'inflow_eu_ua_en_raw'
start_pattern = 'Inflow'
rar_file = 'Inflow_eu_en.rar'
title_str = 'utg_inflow_eu_en'

# Download files
content = BeautifulSoup(requests.get(url).text)
files = [link.get('href') for link in content.find_all('a') if link.get('href').startswith(start_pattern)]
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

# function processing a separate dataset

def get_gas_data(file):
    print(file)
    df_raw = pd.read_excel(os.path.join(dir_raw, file)).iloc[2:,1:3]
    df_raw.columns = ['station', 'import_tsd_m3']
    title = [item for item in df_raw['station'] if pd.notna(item)][0]
    df = df_raw.loc[pd.notnull(df_raw.station)&pd.notnull(df_raw.import_tsd_m3)]
    title = re.findall('\d.*', title)[0]
    df['date'] = title
    df['date'] = pd.to_datetime(df['date'], format = '%d.%m.%Y', exact = True)
    df['date'] = [d.date() for d in df['date']]
    countries_found = [re.findall('\(([^)]+)', s) for s in df['station']]
    df['country'] = reduce(add, [l+[''] if len(l)==0 else l for l in countries_found])
    df['station'] = df['station'].str.replace(r"\(.*\)", "")
    df = df[['date', 'station', 'country', 'import_tsd_m3']]
    df_total = df[df['station'].str.contains('otal')].drop(['station', 'country'], axis = 1)
    df_comp = df[~df['station'].str.contains('otal')]
    df_comp = df_comp[~df_comp['station'].str.contains('station')]
    return (df_total, df_comp)

# joining files into a list
files = os.listdir(dir_raw)
all_dfs = list(map(get_gas_data, files))

# creating and processing full datasets: daily data and totals
all_dfs_total = pd.concat([df[0] for df in all_dfs]).sort_values(['date'])
all_dfs_comp = pd.concat([df[1] for df in all_dfs]).sort_values(['date', 'station'])

# writing into .csv та .xlsx

if not os.path.exists("output"):
    os.makedirs("output")
    
strdate = all_dfs_comp['date'].iloc[-1].strftime("%y%m%d")        

all_dfs_total.to_csv(os.path.join('output', title_str+'_totals_'+strdate+'.csv'), encoding = 'utf-8', index = False)
all_dfs_total.to_excel(os.path.join('output', title_str+'_totals_'+strdate+'.xlsx'), encoding = 'utf-8', index = False)
all_dfs_comp.to_csv(os.path.join('output', title_str+'_daily_'+strdate+'.csv'), encoding = 'utf-8', index = False)
all_dfs_comp.to_excel(os.path.join('output', title_str+'_daily_'+strdate+'.xlsx'), encoding = 'utf-8', index = False)

# check sums & write into .xlsx

grouped_sums = all_dfs_comp.groupby('date')['import_tsd_m3'].agg('sum').round(3).to_frame(name = 'grouped_sum').reset_index()
compare_sums = pd.merge(grouped_sums, all_dfs_total, on = 'date', how = 'outer')
compare_sums.to_excel(os.path.join('output', title_str+'_compare_sums.xlsx'), encoding = 'utf-8', index = False)
