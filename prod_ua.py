#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import re
from numpy import nan


# In[3]:


# Функція отримання даних про видобуток газу з одного файлу (за один день)
# Вимагає завантаження розпакованих файлів Укртрансгазу (архів prod_ua.rar тут: http://utg.ua/wp-content/uploads/cdd/ARCHIVE/PROD/UA/)
# у папку prod_ua_raw, що знаходиться одній папці зі скриптом

def get_gas_data(file):
    print(file)
    df_raw = pd.read_excel(os.path.join('prod_ua_raw', file)).iloc[1:,1:3]
    df_raw.columns = ['company', 'production_tsd_m3']
    title = [item for item in df_raw['company'] if pd.notna(item)][0]
    df = df_raw.loc[pd.notnull(df_raw.company)&pd.notnull(df_raw.production_tsd_m3)]
    title = re.findall('\d.*', title)[0]
    df['date'] = title
    df['date'] = pd.to_datetime(df['date'], format = '%d.%m.%Y', exact = True)
    df['date'] = [d.date() for d in df['date']]
    df = df[['date', 'company', 'production_tsd_m3']]
    df_total = df[df['company'].str.contains('сього')].drop('company', axis = 1)
    df_comp = df[~df['company'].str.contains('сього')]
    return (df_total, df_comp)


# In[4]:


# Закачка даних всіх файлів у спільний список
files = os.listdir('prod_ua_raw')
all_dfs = list(map(get_gas_data, files))


# In[15]:


# Об'єднання в таблиці: окремо для щоденних імпортованих сум і для даних за компаніями
all_dfs_total = pd.concat([df[0] for df in all_dfs]).sort_values('date')
all_dfs_comp = pd.concat([df[1] for df in all_dfs])
all_dfs_comp['company'] = all_dfs_comp['company'].astype('category')
all_dfs_comp['company'].cat.reorder_categories(['Укргазвидобування', 'Укрнафта', 'Інші'], inplace = True)
all_dfs_comp = all_dfs_comp.sort_values(['date', 'company'])


# In[31]:


# Запис файлів у форматах .csv та .xlsx

if not os.path.exists("output"):
    os.makedirs("output")

strdate = all_dfs_comp['date'].iloc[-1].strftime("%y%m%d")        

all_dfs_total.to_csv(os.path.join('output', 'ukrtransgas_daily_totals_'+strdate+'.csv'), encoding = 'utf-8', index = False)
all_dfs_total.to_excel(os.path.join('output','ukrtransgas_daily_totals_'+strdate+'.xlsx'), encoding = 'utf-8', index = False)
all_dfs_comp.to_csv(os.path.join('output', 'ukrtransgas_daily_'+strdate+'.csv'), encoding = 'utf-8', index = False)
all_dfs_comp.to_excel(os.path.join('output','ukrtransgas_daily_'+strdate+'.xlsx'), encoding = 'utf-8', index = False)


# In[32]:


# Перевірка збігу щоденних сум з файлу за компаніями та щоденних сум, вказаних у таблицях Укртрансгазу (запис у файл  Excel)
grouped_sums = all_dfs_comp.groupby('date')['production_tsd_m3'].agg('sum').round(3).to_frame(name = 'grouped_sum').reset_index()
compare_sums = pd.merge(grouped_sums, all_dfs_total, on = 'date', how = 'outer')
print(compare_sums.head(20))
compare_sums.to_excel(os.path.join('output',"ukrtransgas_daily_compare_sums.xlsx"), encoding = 'utf-8', index = False)

