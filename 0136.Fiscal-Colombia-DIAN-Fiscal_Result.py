#!/usr/bin/env python
# coding: utf-8

# In[12]:


# !pip install lxml 
import pandas as pd
import zipfile
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

# In[13]:


documento_url_1 = "https://www.dian.gov.co/dian/cifras/EstadisticasRecaudo/Estad%C3%ADsticas%20de%20recaudo%20mensual%20por%20tipo%20de%20impuesto%202000-2021.zip"
response = requests.request("GET", documento_url_1)
filename = 'COL_FISCAL_1.zip'
if response.status_code == 200:
    open(filename, 'wb').write(response.content)

with zipfile.ZipFile('COL_FISCAL_1.zip', 'r') as zip_ref:
    zip_ref.extractall()
response.status_code


# In[14]:


df_1 = pd.read_excel('Estadísticas de recaudo mensual por tipo de impuesto 2000-2021.xlsx', skiprows=6,header=[0])
index_with = []
for i, item in enumerate(df_1['Año']):
    try:
        year_val = int(item)
    except:
        index_with.append(i)
df_1.drop(index_with,0, inplace=True)

index_list = df_1.index
index_list
for i in index_list:
    year_val = df_1['Año'][i]
    month_val = df_1['Mes'][i]
    if month_val == 'Enero':
        df_1['Año'][i] = str(year_val) + '-01-01'
    elif month_val == 'Febrero':
        df_1['Año'][i] = str(year_val) + '-02-01'
    elif month_val == 'Marzo':
        df_1['Año'][i] = str(year_val) + '-03-01'
    elif month_val == 'Abril':
        df_1['Año'][i] = str(year_val) + '-04-01'
    elif month_val == 'Mayo':
        df_1['Año'][i] = str(year_val) + '-05-01'
    elif month_val == 'Junio':
        df_1['Año'][i] = str(year_val) + '-06-01'
    elif month_val == 'Julio':
        df_1['Año'][i] = str(year_val) + '-07-01'
    elif month_val == 'Agosto':
        df_1['Año'][i] = str(year_val) + '-08-01'
    elif month_val == 'Septiembre':
        df_1['Año'][i] = str(year_val) + '-09-01'
    elif month_val == 'Octubre':
        df_1['Año'][i] = str(year_val) + '-10-01'
    elif month_val == 'Noviembre':
        df_1['Año'][i] = str(year_val) + '-11-01'
    elif month_val == 'Diciembre':
        df_1['Año'][i] = str(year_val) + '-12-01'
df_1 = df_1.drop(columns=['Mes'])
df_1 = df_1.rename(columns={"Año": "Date"})
df_1['Date'] = pd.to_datetime(df_1['Date'], format = '%Y-%m-%d')
df_1 = df_1.set_index('Date')

df_1 = df_1[df_1.index.duplicated()==False]
df_1['country'] = 'Colombia'
df_1


# In[15]:


alphacast.datasets.dataset(136).upload_data_from_df(df_1, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
