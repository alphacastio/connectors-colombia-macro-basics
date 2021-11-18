#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd

import requests
import numpy as np
from datetime import date
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


meses = { 
    "Marzo": 3, 
    "Junio": 6,
    "Septiembre":9,
    "Diciembre": 12
        }


# In[3]:


def fix_date(df):
    if "Año" in df.columns:
        df["Year"] = df["Año"].ffill()
        del df["Año"]
    if "Período" in df.columns:
        df["Year"] = df["Período"].ffill()        
        del df["Período"]

    df["Day"] = 1
    df["Month"] = df["Unnamed: 1"].str.split(" ").str[0].ffill()
    df["Month"] = df["Month"].replace(meses)
    df["Date"] = pd.to_datetime(df[["Year", "Month", "Day"]], errors="coerce")
    
    del df["Year"]
    del df["Month"]
    del df["Day"]
    del df["Unnamed: 1"]
    df = df[df["Date"].notnull()]
    df = df.set_index("Date")
    return df


# In[4]:


df = pd.read_excel("https://www.banrep.gov.co/sites/default/files/paginas/sfis_005.xls", skiprows= 4)
df = df[df["Saldo"].notnull()]
df = fix_date(df)
df.columns = [
                "Gobierno Nacional Central - Saldo Deuda Total",
                "Gobierno Nacional Central - Saldo Deuda Interna de mediano y largo plazo",
                "Gobierno Nacional Central - Saldo Deuda Externa de mediano y largo plazo"
             ]

df_merge = df


# In[5]:


df


# In[6]:


df = pd.read_excel("https://www.banrep.gov.co/sites/default/files/paginas/sfis_007.xls", skiprows= 5)
df = fix_date(df)
df = df[df["Total"].notnull()]
del df["Total"]
df.columns = [
    "Gobierno Nacional Central - Saldo Deuda Interna de mediano y largo plazo - Desembolsos", 
    "Gobierno Nacional Central - Saldo Deuda Interna de mediano y largo plazo - Amortizaciones",
    "Gobierno Nacional Central - Saldo Deuda Interna de mediano y largo plazo - Ajustes"
]

df_merge = df_merge.merge(df, how="left", right_index=True, left_index=True)


# In[7]:


df_merge = df_merge[
      ['Gobierno Nacional Central - Saldo Deuda Total',
       'Gobierno Nacional Central - Saldo Deuda Externa de mediano y largo plazo',
       'Gobierno Nacional Central - Saldo Deuda Interna de mediano y largo plazo',       
       'Gobierno Nacional Central - Saldo Deuda Interna de mediano y largo plazo - Desembolsos',
       'Gobierno Nacional Central - Saldo Deuda Interna de mediano y largo plazo - Amortizaciones',
       'Gobierno Nacional Central - Saldo Deuda Interna de mediano y largo plazo - Ajustes']  
]


# In[10]:


df_merge["country"] = "Colombia"

alphacast.datasets.dataset(191).upload_data_from_df(df_merge, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
