#!/usr/bin/env python
# coding: utf-8

# In[15]:


import requests
import pandas as pd
from zipfile import ZipFile
import io

import requests_html
from requests_html import HTMLSession
import numpy as np
session = HTMLSession ()
import regex as re
from datetime import datetime

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

def get_data(url, skiprows, header=1):    
    r = session.get(url)
    df = pd.read_excel(r.content, skiprows = skiprows, header=header)
    return df


# In[17]:


url = 'https://totoro.banrep.gov.co/analytics/saw.dll?Download&Format=excel2007&Extension=.xlsx&BypassCache=true&path=%2Fshared%2fSeries%20Estad%C3%ADsticas_T%2F1.%20Balanza%20de%20Pagos%2F1.2%20Trimestral%2F%201.2.1.BP_Resumen%20IQY&lang=es&NQUser=publico&NQPassword=publico123&SyncOperation=1%20'

df = get_data(url, 11, [0,1]) 

df = df.dropna(how='all').dropna(how='all',axis=1)
df = df.dropna(how='all', subset=df.columns[1:])

# df.columns = df.columns.astype(str)
# df.columns = pd.Series([np.nan if 'Unnamed:' in x else x for x in df.columns.values]).ffill().values.flatten()

if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.map(' - '.join)

for col in df.columns:
    new_col = re.sub('[()]', '', col).replace("Unnamed: 0_level_0 -", "").replace('r','').replace('p','').replace(' ','')
    df = df.rename(columns={col: new_col})
df


# In[18]:


df["level_1"] = df["Cuenta"]

sublevels = {
    'level_2':["\xa0\xa0\xa0 Crédito (exportaciones)", "\xa0\xa0\xa0 Débito (importaciones)", "\xa0\xa0\xa0\xa0\xa0 1.A Bienes y servicios", "\xa0\xa0\xa0\xa0\xa0 1.B Ingreso primario (Renta factorial)", "\xa0\xa0\xa0\xa0\xa0 1.C Ingreso secundario (Transferencias corrientes)", 
               "\xa0\xa0\xa0\xa0\xa0 3.1 Inversión directa", "\xa0\xa0\xa0\xa0\xa0 3.2 Inversión de cartera", "\xa0\xa0\xa0\xa0\xa0 3.3 Derivados financieros (distintos de reservas) y opciones de compra de acciones por parte de empleados", 
                "\xa0\xa0\xa0\xa0\xa0 3.4 Otra inversión",  "\xa0\xa0\xa0\xa0\xa0 3.5 Activos de reserva" ],
    'level_3':["\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0 Crédito (exportaciones)", "\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0 Débito (importaciones)", "\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0 Crédito", "\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0 Débito",
               "\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0 Adquisición neta de activos financieros", "\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0 Pasivos netos incurridos", "\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0 1.A.a Bienes", "1.A.b Servicios (*)"],
    'level_4':["\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0 Crédito (exportaciones)", "\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0 Débito (importaciones)", "\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0 3.1.1 Participaciones de capital y participaciones en fondos de inversión",
               "\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0 3.1.2 Instrumentos de deuda", "\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0 3.2.1 Participaciones de capital y participaciones en fondos de inversión", "\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0\xa0 3.2.2 Títulos de deuda"],     
} 

for sublevel in sublevels:
    df[sublevel] = np.nan
    print(sublevel)
    df.loc[df["level_1"].isin(sublevels[sublevel]), sublevel] = df["level_1"]
    df.loc[df["level_1"].isin(sublevels[sublevel]), "level_1"] = np.nan 
df


# In[19]:


for x in range(len(sublevels)+1,0,-1):
    print(x)
    df["level_{}_temp".format(x)] = df["level_{}".format(x)].ffill()
    df.loc[df["level_{}".format(x)].notnull(), "level_{}".format(x)] = df["level_{}_temp".format(x).strip().replace("\\xa0", "")]
    for y in range(1, x):
        df.loc[df["level_{}".format(y)].notnull(), "level_{}_temp".format(x)] = np.nan
        print(str(x) + " - " + str(y))


# In[20]:


df["concept"] = df["level_1_temp"]
for y in range (2,len(sublevels)+2):
    df.loc[df["level_{}_temp".format(y)].notnull(), "concept"] = df["concept"] + " - " + df["level_{}_temp".format(y)]
    print(y)
df


# In[21]:


df = df[df.columns.drop(list(df.filter(regex='level')))]
# df = df.rename(columns = {'concept': 'Concepto'})
df


# In[22]:


df = df.set_index("concept")
df = df.drop('Cuenta',axis=1)
df1 = df.transpose()
df1 = df1.reset_index()
pd.set_option("display.max_columns", None)
df1


# In[23]:


def dateWrangler(x):
        x=str(x)
        list_x = x.split('-')
        if list_x[1] == 'I':
            y = list_x[0]+'-01-01'
        elif list_x[1] == 'II':
            y = list_x[0]+'-04-01'
        elif list_x[1] == 'III':
            y = list_x[0]+'-07-01'
        elif list_x[1] == 'IV':
            y = list_x[0]+'-10-01'
        return y

df1['Date'] = df1['index'].apply(lambda x: dateWrangler(x))
df1['Date'] = pd.to_datetime(df1['Date'])
del df1["index"]
df1 = df1.set_index("Date")


# In[24]:



df1


# In[25]:


colnames_Dict = {
    '1 Cuenta corriente':'Cuenta corriente',
    '1 Cuenta corriente -     Crédito (exportaciones)':'Cuenta corriente - Crédito (exportaciones)',
    '1 Cuenta corriente -     Débito (importaciones)':'Cuenta corriente - Débito (importaciones)',
    '1 Cuenta corriente -       1.A Bienes y servicios': 'Cuenta corriente - Bienes y servicios'
}
df1 = df1.rename(columns=colnames_Dict)

# for col in df1.columns:
#         new_col = re.sub(' +', ' ', col).replace("1", "").replace("2", "").replace("3", "").replace(".1", "").replace(".4", "").replace(".A", "").replace(".B", "").replace(".a", "").replace(".5", "").replace(".C", "").replace(".b", "").replace("(*)", "")
#         df1 = df1.rename(columns={col: new_col})  

# df1.columns = list(df1.columns)        
        
df1


# In[28]:


df1['country'] = 'Colombia'
for col in df1.columns:
    df1 = df1.rename(columns={col: col.replace("\xa0", "")})


alphacast.datasets.dataset(325).upload_data_from_df(df1, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
