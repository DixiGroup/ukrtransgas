#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import re
from numpy import nan
from functools import reduce
from operator import add


# In[2]:


# Функція отримання даних про імпорт газу з одного файлу (за один день)
# Вимагає завантаження розпакованих файлів Укртрансгазу (архів Inflow_eu_ua.rar тут: http://utg.ua/wp-content/uploads/cdd/ARCHIVE/INFLOW_EU/UA/)
# у папку inflow_eu_ua_raw, що знаходиться одній папці зі скриптом

def get_gas_data(file):
    print(file)
    df_raw = pd.read_excel(os.path.join('inflow_eu_ua_raw', file)).iloc[1:,1:3]
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
    df_total = df[df['station'].str.contains('сього')].drop(['station', 'country'], axis = 1)
    df_comp = df[~df['station'].str.contains('сього')]
    return (df_total, df_comp)


# In[3]:


# Закачка даних всіх файлів у спільний список
files = os.listdir('inflow_eu_ua_raw')
all_dfs = list(map(get_gas_data, files))


# In[8]:


# Об'єднання в таблиці: окремо для щоденних імпортованих сум і для даних за станціями
all_dfs_total = pd.concat([df[0] for df in all_dfs]).sort_values(['date'])
all_dfs_comp = pd.concat([df[1] for df in all_dfs]).sort_values(['date', 'station'])


# In[11]:


# Запис файлів у форматах .csv та .xlsx

if not os.path.exists("output"):
    os.makedirs("output")
    
strdate = all_dfs_comp['date'].iloc[-1].strftime("%y%m%d")        

all_dfs_total.to_csv(os.path.join('output', 'ukrtransgas_import_totals_'+strdate+'.csv'), encoding = 'utf-8', index = False)
all_dfs_total.to_excel(os.path.join('output','ukrtransgas_import_totals_'+strdate+'.xlsx'), encoding = 'utf-8', index = False)
all_dfs_comp.to_csv(os.path.join('output', 'ukrtransgas_import_'+strdate+'.csv'), encoding = 'utf-8', index = False)
all_dfs_comp.to_excel(os.path.join('output','ukrtransgas_import_'+strdate+'.xlsx'), encoding = 'utf-8', index = False)


# In[12]:


# Перевірка збігу щоденних сум з файлу за компаніями та щоденних сум, вказаних у таблицях Укртрансгазу (запис у файл  Excel)

grouped_sums = all_dfs_comp.groupby('date')['import_tsd_m3'].agg('sum').round(3).to_frame(name = 'grouped_sum').reset_index()
compare_sums = pd.merge(grouped_sums, all_dfs_total, on = 'date', how = 'outer')
print(compare_sums.head(20))
compare_sums.to_excel(os.path.join('output',"ukrtransgas_import_daily_compare_sums.xlsx"), encoding = 'utf-8', index = False)

