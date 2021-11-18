#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from zipfile import ZipFile
import io

import requests_html
from requests_html import HTMLSession
import numpy as np
session = HTMLSession ()

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


def get_data(url, date_col, skiprows, header=1):    
    r = session.get(url)
    df = pd.read_excel(r.content, skiprows = skiprows, header=header)
    df["Date"] = pd.to_datetime(df[date_col], errors="coerce")
    df = df[df["Date"].notnull()]
    df = df.set_index("Date")
    del df[date_col]
    return df

url = "https://totoro.banrep.gov.co/analytics/saw.dll?Download&Format=excel2007&Extension=.xlsx&BypassCache=true&path={}&lang=es&NQUser=publico&NQPassword=publico123&SyncOperation=1"


# In[3]:


path = "%2Fshared%2FSeries%20Estad%C3%ADsticas_T%2F1.%20Inversi%C3%B3n%20directa%2F1.1%20Historico%2F1.1.3%20Inversion%20extranjera%20directa%20en%20Colombia%20-%20Actividad%20economica_Trimestral_IQY"
# print(url.format(path))

df = get_data(url.format(path),"Año(aaaa)-Trimestre(t)", 5) 
df_merge = df
# print(url.format(path))
df

df = df.reset_index()

df = df.sort_values(by=["Date"])

# df = df[df.columns.drop(list(df.filter(regex='Provisional')))]

import re

for col in df.columns:
        new_col = re.sub(' +', ' ', col).replace(" ³", "")
        df = df.rename(columns={col: new_col}) 

del df['Estado de la información']

df['Date'] = df['Date'].astype(str)

df['Date'].dtype

df['Date'] = df['Date'].str.replace(
                "-04-01", "-10-01")

df['Date'] = df['Date'].str.replace(
                "-02-01", "-04-01")

df['Date'] =  df['Date'].str.replace(
                "-03-01", "-07-01")

df['Date']=pd.to_datetime(df['Date'])

df = df.reset_index().set_index('Date')

df['country'] = 'Colombia'

del df['index']

df


# In[5]:



alphacast.datasets.dataset(313).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
