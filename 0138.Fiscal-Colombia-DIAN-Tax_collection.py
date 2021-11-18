#!/usr/bin/env python
# coding: utf-8

# In[6]:


# !pip install lxml 
import pandas as pd
import requests, json
import numpy as np
# from urllib.request import urlopen
from lxml import etree
from collections import OrderedDict
from datetime import datetime

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[7]:


df_obj = OrderedDict()

#sfis_003_oficial
documento_url_1 = "https://www.banrep.gov.co/sites/default/files/paginas/sfis_003_oficial.xls"
df_1 = pd.read_excel(documento_url_1, skiprows=4,header=[0])
df_obj['sfis_003_oficial'] = df_1

#sfis_003
documento_url_2 = "https://www.banrep.gov.co/sites/default/files/paginas/sfis_003.xls"
df_1 = pd.read_excel(documento_url_1, skiprows=4,header=[0])
df_obj['sfis_003'] = df_1
df_1


# In[8]:


df_obj_result = OrderedDict()
for sheet_name in df_obj.keys():
    df_1 = df_obj[sheet_name]
    df_2 = df_1[df_1['Unnamed: 1'].notnull()]

    index_list_for_delete = []
    index_list = df_2.index
    for i in index_list:
        try:
            year_val = int(df_2['Unnamed: 1'][i])
            index_list_for_delete.append(i)
        except:
            pass
        if i > 368:
            index_list_for_delete.append(i)
    df_2.drop(index_list_for_delete,0, inplace=True)

    index_list = df_2.index
    for i in index_list:
        try:
            year_val = int(df_2['Período'][i])
        except:
            pass
        month_val = df_1['Unnamed: 1'][i].replace('(pr)', '').strip()
        if month_val == 'Enero':
            df_2['Unnamed: 1'][i] = str(year_val) + '-01-01'
        elif month_val == 'Febrero':
            df_2['Unnamed: 1'][i] = str(year_val) + '-02-01'
        elif month_val == 'Marzo':
            df_2['Unnamed: 1'][i] = str(year_val) + '-03-01'
        elif month_val == 'Abril':
            df_2['Unnamed: 1'][i] = str(year_val) + '-04-01'
        elif month_val == 'Mayo':
            df_2['Unnamed: 1'][i] = str(year_val) + '-05-01'
        elif month_val == 'Junio':
            df_2['Unnamed: 1'][i] = str(year_val) + '-06-01'
        elif month_val == 'Julio':
            df_2['Unnamed: 1'][i] = str(year_val) + '-07-01'
        elif month_val == 'Agosto':
            df_2['Unnamed: 1'][i] = str(year_val) + '-08-01'
        elif month_val == 'Septiembre':
            df_2['Unnamed: 1'][i] = str(year_val) + '-09-01'
        elif month_val == 'Octubre':
            df_2['Unnamed: 1'][i] = str(year_val) + '-10-01'
        elif month_val == 'Noviembre':
            df_2['Unnamed: 1'][i] = str(year_val) + '-11-01'
        elif month_val == 'Diciembre':
            df_2['Unnamed: 1'][i] = str(year_val) + '-12-01'
    df_2 = df_2.drop(columns=['Período'])
    df_2 = df_2.rename(columns={"Unnamed: 1": "Date"})
    df_obj_result[sheet_name] = df_2
df_obj_result['sfis_003_oficial']


# In[9]:


df = df_obj_result['sfis_003']
df = df.set_index('Date')
df['country'] = 'Colombia'
df


# In[10]:


alphacast.datasets.dataset(138).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
