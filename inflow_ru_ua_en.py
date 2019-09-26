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

# constants for downloading
url = 'http://utg.ua/wp-content/uploads/cdd/ARCHIVE/INFLOW/EN/'
dir_raw = 'inflow_ru_ua_en_raw'
start_pattern = 'Inflow'
rar_file = 'Inflow_en.rar'

# constants for data processing
title_str = 'utg_inflow_ru_en'
countries_dict = {"Uzhgorod": "Slovakia", 
                  "Beregovo": "Hungary",
                  "Drozdovichi": "Poland",
                  "Orlovka": "Romania",
                  "Tekovo": "Romania",
                  "Moldova": "Moldova"}
dir_dict = {"Flow total": "in", "Inflow total": "in", "Transit total": "out"}
col_dict = {"Flow total": "inflow", "Inflow total": "inflow", "Transit total": "transit"}

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
    df_raw = pd.read_excel(os.path.join(dir_raw, file)).iloc[1:,1:3]
    df_raw.columns = ['station', 'volume_tsd_m3']
    title = [item for item in df_raw['station'] if pd.notna(item)][0].replace('Russian gas inflow and transit ', '')
    df = df_raw.loc[pd.notnull(df_raw.station)&pd.notnull(df_raw.volume_tsd_m3)]
    df['date'] = title
    df['date'] = pd.to_datetime(df['date'], format = '%d.%m.%Y', exact = True)
    df['date'] = [d.date() for d in df['date']]
    countries_found = [re.findall('\(.*\)$', s) for s in df['station']]
    df['country'] = reduce(add, [l+[''] if len(l)==0 else l for l in countries_found])
    df['country'] = df['country'].str.replace('.* ', '').str.replace(r"\(|\)|[Tt]otal", "")
    df['temp'] = df['station'].str.extract(r'([A-Z][a-z]+)')
    df['country_out'] = df['temp'].map(countries_dict)
    df['country'][df['country']==''] = df['country_out'][df['country']=='']
    df['station'] = df['station'].str.replace(r' \([A-Z][a-z]{2,}\)', '').str.strip()
    df['direction'] = df['station'].str.strip().map(dir_dict).ffill()
    df.loc[df['station'].str.contains('OBA|ОВА|Gazprom|Russian gas'), 'direction'] = ''
    df['volume_tsd_m3'][df['direction']=='out'] = 0-df['volume_tsd_m3'][df['direction']=='out'] 
    df_total = df[df['station'].str.contains('Flow total|Inflow total|Transit total|Gazprom|Russian gas|OBA|ОВА')]
    df_comp = df[~df['station'].str.contains('Flow total|Inflow total|Transit total|Gazprom|Russian gas|OBA|ОВА')]
    df_comp = df_comp[['date', 'direction', 'station', 'country', 'volume_tsd_m3']]
    df_total = df_total[['date', 'station', 'volume_tsd_m3']]
    return (df_total, df_comp)

# joining files into a list
files = os.listdir(dir_raw)
all_dfs = list(map(get_gas_data, files))

# creating and processing full datasets: daily data and totals

all_dfs_total = pd.concat([df[0] for df in all_dfs]).sort_values('date', kind = 'mergesort')
all_dfs_comp = pd.concat([df[1] for df in all_dfs]).sort_values('date', kind = 'mergesort')
all_dfs_total['temp'] = all_dfs_total['station'].map(col_dict)
all_dfs_total.loc[pd.isnull(all_dfs_total['temp']), 'temp'] = "balance_official"
all_dfs_total = all_dfs_total.pivot(index = 'date', columns = 'temp', values = 'volume_tsd_m3')
all_dfs_total.reset_index(inplace = True)
all_dfs_total['change_calculated'] = all_dfs_total['inflow']+all_dfs_total['transit']
all_dfs_total = all_dfs_total[['date', 'inflow', 'transit', 'change_calculated', 'balance_official']]

# writing into .csv та .xlsx

if not os.path.exists("output"):
    os.makedirs("output")
    
strdate = all_dfs_comp['date'].iloc[-1].strftime("%y%m%d")     
    
all_dfs_total.to_csv(os.path.join('output', title_str+'_totals_'+strdate+'.csv'), encoding = 'utf-8', index = False)
all_dfs_total.to_excel(os.path.join('output', title_str+'_totals_'+strdate+'.xlsx'), encoding = 'utf-8', index = False)
all_dfs_comp.to_csv(os.path.join('output', title_str+'_daily_'+strdate+'.csv'), encoding = 'utf-8', index = False)
all_dfs_comp.to_excel(os.path.join('output', title_str+'_daily_'+strdate+'.xlsx'), encoding = 'utf-8', index = False)

# check sums & write into .xlsx

grouped_sums = all_dfs_comp.groupby(['date', 'direction'])['volume_tsd_m3'].agg('sum').round(3).to_frame(name = 'grouped_sum').reset_index()
total_to_compare = pd.melt(all_dfs_total.iloc[:,:3], id_vars = 'date', var_name = 'direction', value_name = 'volume_tsd_m3')
total_to_compare['direction'] = total_to_compare['direction'].map({'inflow': 'in', 'transit': 'out'})
compare_sums = pd.merge(grouped_sums, total_to_compare, on = ['date', 'direction'], how = 'outer').sort_values(['date', 'direction'])
compare_sums.to_excel(os.path.join('output', title_str+'compare_sums.xlsx'), encoding = 'utf-8', index = False)
