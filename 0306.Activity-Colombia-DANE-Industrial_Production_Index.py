#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests

from datetime import datetime
from urllib.request import urlopen
from lxml import etree
import io
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[3]:


# get html from site and write to local file
url = "https://www.dane.gov.co/index.php/estadisticas-por-tema/industria/indice-de-produccion-industrial-ipi"
response = requests.get(url,verify=False)
html = response.content
htmlparser = etree.HTMLParser()
tree = etree.fromstring(html, htmlparser)
xls_address = tree.xpath("//*[@id='cnpv']/div/div/table[1]/tbody/tr/td/table/tbody/tr[2]/td[4]/a/@href")[0]
xls_address


# In[4]:


url = 'https://www.dane.gov.co/' + xls_address
r = requests.get(url, allow_redirects=True, verify=False)

df = pd.read_excel(r.content, skiprows=8, sheet_name='3. Indices total por clase ')
df = df.dropna(how='all').dropna(how='all', subset = df.columns[1:])
df = df.reset_index()
df[['Dominios','Clases industriales']] = df[['Dominios','Clases industriales']].astype(str)

df['Clases Industriales'] = df['Dominios'] + ' - ' + df['Clases industriales']

df['Año'] = df['Año'].astype(str).apply(lambda x: x.replace('.0', ''))
df['Mes'] = df['Mes'].astype(str).apply(lambda x: x.replace('.0', ''))

df['Mes'] = df['Mes'].replace(
        {
            '1': "01-01",
            '2': "02-01",
            '3': "03-01",
            '4': "04-01",
            '5': "05-01",
            '6': "06-01",
            '7': "07-01",
            '8': "08-01",
            '9': "09-01",
            '10': "10-01",
            '11': "11-01",
            '12': "12-01",

        })

df['Date'] = df['Año'] + '-' + df['Mes']
df['Date'] = df['Date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
df = df.drop(['index','Año','Mes','Dominios','Clases industriales'], axis=1)

df = df.set_index(['Date', 'Clases Industriales'])
df = df.unstack('Clases Industriales')

for col in df.columns:
    if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.map(' - '.join)

df


# In[5]:


df['country'] = 'Colombia'

alphacast.datasets.dataset(306).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

