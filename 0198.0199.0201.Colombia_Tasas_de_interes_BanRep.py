#!/usr/bin/env python
# coding: utf-8

# In[16]:


import requests
import pandas as pd
from zipfile import ZipFile
import io

from requests_html import HTMLSession
import numpy as np
session = HTMLSession ()
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[17]:


def get_data(url, date_col, skiprows, header=1):    
    r = session.get(url)
    df = pd.read_excel(r.content, skiprows = skiprows, header=header)
    df["Date"] = pd.to_datetime(df[date_col], errors="coerce")
    df = df[df["Date"].notnull()]
    df = df.set_index("Date")
    del df[date_col]
    return df

url = "https://totoro.banrep.gov.co/analytics/saw.dll?Download&Format=excel2007&Extension=.xlsx&BypassCache=true&path={}&lang=es&NQUser=publico&NQPassword=publico123&SyncOperation=1"


# In[18]:


path = "%2Fshared%2FSeries%20Estad%C3%ADsticas_T%2F1.%20Tasa%20de%20intervenci%C3%B3n%20de%20pol%C3%ADtica%20monetaria%2F1.2.TIP_Serie%20historica%20diaria"
df = get_data(url.format(path), "Fecha (dd/mm/aaaa)", 6) 
df_merge = df


# In[19]:


path = "%2Fshared%2fSeries%20Estad%c3%adsticas_T%2F1.%20Tasa%20Interbancaria%20%28TIB%29%2F1.1.TIB_Serie%20historica%20IQY"
df = get_data(url.format(path), "Fecha(dd/mm/aaaa)", 5)
#del df["Unnamed: 3"]
df
df.columns = ["Tasa interbancaria (TIB)", "Monto"]
df_merge = df_merge.merge(df, how="outer", left_index=True, right_index=True)


# In[20]:


path = "%2Fshared%2fSeries%20Estad%c3%adsticas_T%2F1.%20IBR%2F%201.1.IBR_Plazo%20overnight%20nominal%20para%20un%20rango%20de%20fechas%20dado%20IQY"
df = get_data(url.format(path), "Fecha (dd/mm/aaaa)", 7)
df = df[["IBR"]]
df.columns = ["Indicador bancario de referencia (IBR) - Overnight"]
df_merge = df_merge.merge(df, how="outer", left_index=True, right_index=True)


# In[21]:


path = "%2Fshared%2fSeries%20Estad%c3%adsticas_T%2F1.%20IBR%2F%201.2.IBR_Plazo%20un%20mes%20nominal%20para%20un%20rango%20de%20fechas%20dado%20IQY"
df = get_data(url.format(path), "Fecha (dd/mm/aaaa)", 7)
df = df[["IBR"]]
df.columns = ["Indicador bancario de referencia (IBR) - Un mes"]
df_merge = df_merge.merge(df, how="outer", left_index=True, right_index=True)


# In[22]:


path = "%2Fshared%2fSeries%20Estad%c3%adsticas_T%2F1.%20IBR%2F%201.3.IBR_Plazo%20tres%20meses%20nominal%20para%20un%20rango%20de%20fechas%20dado%20IQY"
df = get_data(url.format(path), "Fecha (dd/mm/aaaa)", 7)
df = df[["IBR"]]
df.columns = ["Indicador bancario de referencia (IBR) - Tres meses"]
df_merge = df_merge.merge(df, how="outer", left_index=True, right_index=True)


# In[23]:


path = "%2fshared%2fSeries%20Estad%C3%ADsticas_T%2f1.%20IBR%2f1.5.IBR_Plazo%20seis%20meses%20nominal%20para%20un%20rango%20de%20fechas%20dado%20IQY"
df = get_data(url.format(path), "Fecha (dd/mm/aaaa)", 7)
df = df[["IBR"]]
df.columns = ["Indicador bancario de referencia (IBR) - Seis Meses"]
df_merge = df_merge.merge(df, how="outer", left_index=True, right_index=True)


# In[24]:


path = "%2Fshared%2FSeries%20Estad%C3%ADsticas_T%2F1.%20Tasas%20de%20Captaci%C3%B3n%2F1.1%20Serie%20empalmada%2F1.1.2%20Semanales%2F1.1.2.1%20DTF%2CCDT%20180%20d%C3%ADas%2CCDT%20360%20d%C3%ADas%20y%20TCC%20-%20(Desde%20el%2012%20de%20enero%20de%201984)%2F1.1.2.1.1.TCA_Para%20un%20rango%20de%20fechas%20dado%20IQY"
df = get_data(url.format(path), "Vigencia desde (dd/mm/aaaa)", 10)
df = df[["DTF %", "CDT 180 %", "CDT 360 %","TCC %"]]
df_merge = df_merge.merge(df, how="outer", left_index=True, right_index=True)


# In[25]:


for col in df_merge.columns:
    if df_merge[col].dtype == "object":
        df_merge[col] = df_merge[col].replace(" ","").str.replace("%","")
        df_merge[col] = df_merge[col].replace("N.A.",np.nan)
        df_merge[col] = df_merge[col].str.replace(",",".")
        df_merge[col] = pd.to_numeric(df_merge[col])


# In[26]:


df_merge["country"] = "Colombia"

alphacast.datasets.dataset(198).upload_data_from_df(df_merge, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


# In[27]:


path = "%2Fshared%2FSeries%20Estad%C3%ADsticas_T%2F1.%20Agregados%20monetarios%2f1.%20Semanal%2f1.1.%20Depositos%20en%20el%20Sistema%20Financiero%20(Pasivos%20sujetos%20a%20encaje%3a%20PSE)_IQY"
r = session.get(url.format(path))
df = pd.read_excel(r.content, skiprows = 3, header=[3,4,5])
if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.map(' - '.join)
date_col = "Unnamed: 0_level_0 - Unnamed: 0_level_1 - Fecha (dd/mm/aaaa)"
df["Date"] = pd.to_datetime(df[date_col], errors="coerce")
df = df[df["Date"].notnull()]
df = df.set_index("Date")
del df[date_col]
df["country"] = "Colombia"

alphacast.datasets.dataset(199).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


def fix_date(df):
    df["Day"] = 1
    df["Year"] = df["Year"].ffill().astype("str").str.replace(" \(p\)", "")
    df["Month"] = df["Month"].str.strip().replace(
        {'Abril': 4, 'Agosto': 8, 
         'Diciembre':12, 'Enero': 1, 
         'Febrero': 2,'Julio': 7, 
         'Junio': 6, 'Marzo': 3, 
         'Mayo': 5 , 'Noviembre': 11, 
         'Octubre': 10, 'Septiembre':9 }
        )
    df["Date"] = pd.to_datetime(df[["Year", "Month", "Day"]], errors="coerce")
    del df["Year"]
    del df["Month"]
    del df["Day"]
    df = df[df["Date"].notnull()]
    df = df.set_index("Date")
    return df
headers = {

"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
"accept-encoding": "gzip, deflate, br", 
"accept-language": "en-US,en;q=0.9,es-ES;q=0.8,es;q=0.7", 
"cache-control": "no-cache", 
"pragma": "no-cache", 
"referer": "https://www.banrep.gov.co/es/estadisticas/indicador-bancario-referencia-ibr", 
"sec-ch-ua": '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"', 
"sec-ch-ua-mobile": "?0", 
"sec-fetch-dest": "document", 
"sec-fetch-mode": "navigate", 
"sec-fetch-site": "same-origin", 
"sec-fetch-user": "?1", 
"upgrade-insecure-requests": "1", 
"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36", 
}

url = "https://www.banrep.gov.co/sites/default/files/srea_010_hist_1.xls"
r = session.get(url, headers= headers)
df = pd.read_excel(r.content, skiprows = 0, header=[3])
df.columns = ["Year", "Month", 
              "Real Wage - Industry - Total - Base 1990", 
              "Real Wage - Industry - No coffee threshing - Base 1990", 
              "Real Wage - Industry - Total - Base 2014"]
df = fix_date(df)
df_merge = df

url = "https://www.banrep.gov.co/sites/default/files/paginas/srea_010.xls"
r = session.get(url, headers= headers)
df = pd.read_excel(r.content)
df.columns = ["Year", "Month", 
                  "Real Wage - Industry - Total - Base 2018"
             ]
df = fix_date(df)
df_merge = df_merge.merge(df, how="outer", left_index=True, right_index=True)

url = "https://www.banrep.gov.co/sites/default/files/srea_011_hist_new.xls"
r = session.get(url)
df = pd.read_excel(r.content, skiprows = 0, header=[3])
df.columns = ["Year", "Month", 
                  "Nominal Wage - Retail - Base 1990", 
                  "Nominal Wage - Retail - Base 1999", 
                  "Nominal Wage - Retail - Base 2013", 
                  "Nominal Wage - Industry - No coffee threshing - Employees - Base 1990",
                  "Nominal Wage - Industry - No coffee threshing - Workers - Base 1990", 
                  "Nominal Wage - Industry - Total - Base 1994", 
             ]
df = fix_date(df)
df_merge = df_merge.merge(df, how="outer", left_index=True, right_index=True)


url = "https://www.banrep.gov.co/sites/default/files/srea_011_new.xls"
r = session.get(url)
df = pd.read_excel(r.content, skiprows = 0, header=[3])
df.columns = ["Year", "Month", 
                  "Nominal Wage - Retail - Base 2019", 
                  "Nominal Wage - Retail - Base 2013_2", 
                  "Nominal Wage - Industry - Total - Base 2014", 
             ]
df = fix_date(df)
df_merge = df_merge.merge(df, how="outer", left_index=True, right_index=True)



df_merge = df_merge.replace("n.d.", np.nan)
df_merge["country"] = "Colombia"


alphacast.datasets.dataset(201).upload_data_from_df(df_merge, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




