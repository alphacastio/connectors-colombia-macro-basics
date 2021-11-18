#!/usr/bin/env python
# coding: utf-8

# In[23]:


import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import numpy as np
import re

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[24]:


#Descargo la página y la parseo
page=requests.get('https://www.dane.gov.co/index.php/estadisticas-por-tema/'
                  'precios-y-costos/indice-de-precios-al-consumidor-ipc')
soup = BeautifulSoup(page.content, 'html.parser')


# In[25]:


#Guardo los links en una lista. Se busca por string en el texto visible de la página
xls_files = []

for link in soup.find_all('a'):
    if 'Anexos' in link.get_text() or 'series de empalme' in link.get_text():
        xls_files.append('https://www.dane.gov.co' + link.get('href'))


# In[26]:


sheets = ['10','11','12','13','14','15','16']
skiprows = [6,5,6,6,6,6]

worksheets = {
    10: {
        "skiprows": 6,
        "prefix": 'IPC sin alimentos'
    }, 
    
    11: {
        "skiprows": 5,
        "prefix": 'IPC de energéticos'
    }, 
    12: {
        "skiprows": 6,
        "prefix": 'IPC total menos energéticos y alimentos'
    }, 
    13: {
        "skiprows": 6,
        "prefix": 'IPC de servicios'
    }, 
    14: {
        "skiprows": 6,
        "prefix": 'IPC de bienes durables'
    }, 
    15: {
        "skiprows": 6,
        "prefix": 'IPC de bienes semi-durables'
    }, 
    16: {
        "skiprows": 6,
        "prefix": 'IPC de bienes no durables'
    }
    
}    


# In[27]:


r = requests.get(xls_files[0], allow_redirects=False, verify=False)

dfFinal = pd.DataFrame()


# In[28]:


def fix_month(df, month_field):
    df[month_field] = df[month_field].replace(
    {
    "Enero": "01-01",
    "Febrero": "02-01",
    "Marzo": "03-01",
    "Abril": "04-01",
    "Mayo": "05-01",
    "Mayo ": "05-01",
    "Junio": "06-01",
    "Julio": "07-01",
    "Agosto": "08-01",
    "Septiembre": "09-01",
    "Octubre": "10-01",
    "Noviembre": "11-01",
    "Diciembre": "12-01",

    })
    return df


# In[29]:


for key in worksheets.keys():

    df = pd.read_excel(r.content, skiprows=worksheets[key]["skiprows"], sheet_name=str(key),header=[0,1])
    df = df.drop(columns=df.columns[6:]) 
    df = df.dropna(how='all').dropna(how='all', subset= df.columns[1:])

    if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.map(' - '.join)

    for col in df.columns:
        new_col = re.sub(' +', ' ', col).replace(" - Unnamed: 0_level_1", "").replace("- Unnamed: 1_level_1","").replace(" - Unnamed: 2_level_1","")
        df = df.rename(columns={col: new_col})
        
    
    df = fix_month(df, 'Mes ')
    df['Año'] = df['Año'].astype(str).apply(lambda x: x.replace('.0', ''))
    df['Date'] = df['Año'] + ' - ' + df['Mes ']
    del df['Año']
    del df['Mes '] 

    df['Date']=pd.to_datetime(df['Date'])
    df = df.set_index('Date')

    for col in df.columns:
        df = df.rename(columns={col: worksheets[key]["prefix"] + ' - ' + col})

    dfFinal = dfFinal.merge(df, how='outer', left_index=True, right_index=True)


# In[30]:


r = requests.get(xls_files[1], allow_redirects=False, verify=False)
df = pd.read_excel(r.content, skiprows=8)
df = fix_month(df, "Mes")
#df = df.set_index("Mes")
df = df.melt("Mes")
df["Date"] = df["variable"].astype("str") + "-" + df["Mes"]
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df[df["Date"].notnull()]
df = df[df["value"].notnull()]
del df["Mes"]
del df["variable"]
df = df.set_index("Date")
df = df.rename(columns={"value": "Nivel general - Indice de precios"})


# In[31]:


dfFinal = dfFinal.merge(df, how="outer", left_index=True, right_index=True)
dfFinal['country'] = 'Colombia'


alphacast.datasets.dataset(303).upload_data_from_df(dfFinal, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




