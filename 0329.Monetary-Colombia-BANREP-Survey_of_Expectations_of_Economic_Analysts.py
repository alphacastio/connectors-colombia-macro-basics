#!/usr/bin/env python
# coding: utf-8

# In[50]:


import pandas as pd
from datetime import datetime
import requests

import re

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


loop = {
    1:{"sheet":'INFLACION_TOTAL', "prefix":'INFLACIÓN_TOTAL'},
    2:{"sheet":'INFLACION_SIN_ALIMENTOS', "prefix":'INFLACIÓN_SIN_ALIMENTOS'},
}

dfFinal = pd.DataFrame()        
for key in loop.keys():

    df = pd.read_excel('https://www.banrep.gov.co/sites/default/files/paginas/series%20historicas.xls',skiprows = 0, sheet_name = loop[key]["sheet"], header=[0,1])

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.map(' - '.join)

    for col in df.columns:
        newCols=[]
        new_col = re.sub(' +', ' ', col).replace("Unnamed: 0_level_0 - ", "")
        df = df.rename(columns={col: new_col})    

    df = df[df.columns.drop(list(df.filter(regex='.1')))]    

    df = df.rename({'FECHA':'Date'}, axis=1)
    
    df['Date']=pd.to_datetime(df['Date'])
    
    df = df.set_index('Date')
    
    for col in df.columns:
        df = df.rename(columns={col: loop[key]["prefix"] + ' - ' + col})
    
    dfFinal = dfFinal.merge(df, how='outer', left_index=True, right_index=True)
    
dfFinal


# In[51]:


df1 = pd.read_excel('https://www.banrep.gov.co/sites/default/files/paginas/series%20historicas.xls',skiprows = 0, sheet_name = 'TRM', header=[0,1])

if isinstance(df1.columns, pd.MultiIndex):
     df1.columns = df1.columns.map(' - '.join)

for col in df1.columns:
        new_col = re.sub(' +', ' ', col).replace("Unnamed: 0_level_0 - ", "").replace("*", "")
        df1 = df1.rename(columns={col: new_col})    

df1 = df1.rename({'Unnamed: 0_level_1':'Date'},axis=1)

df1 = df1.dropna(how='all', subset=df1.columns[1:])

df1 = df1.dropna(how='all', axis=1)

df1['Date']=pd.to_datetime(df1['Date'])

df1 = df1.set_index('Date')

for col in df1.columns:
        df1 = df1.rename(columns={col: 'TRM' + ' - ' + col})

df1


# In[52]:


df2 = pd.read_excel('https://www.banrep.gov.co/sites/default/files/paginas/series%20historicas.xls',skiprows = 0, sheet_name ='TASA DE INTERVENCION BR', header=[0,1])

if isinstance(df2.columns, pd.MultiIndex):
     df2.columns = df2.columns.map(' - '.join)

for col in df2.columns:
        new_col = re.sub(' +', ' ', col).replace("Unnamed: 0_level_0 - ", "").replace("*", "")
        df2 = df2.rename(columns={col: new_col})    

df2 = df2.rename({'Fecha':'Date'},axis=1)

df2 = df2.dropna(how='all', subset=df2.columns[1:])

df2 = df2.dropna(how='all', axis=1)

df2['Date']=pd.to_datetime(df2['Date'])

df2 = df2.set_index('Date')

for col in df2.columns:
        df2 = df2.rename(columns={col: 'TASA DE INTERVENCION BR' + ' - ' + col})

df2


# In[53]:


dfFinal1 = dfFinal.merge(df1 , how='outer', left_index=True, right_index=True)

dfFinal1


# In[54]:


dfFinal2 = dfFinal1.merge(df2 , how='outer', left_index=True, right_index=True) 

dfFinal2.columns = map(str.title, dfFinal2.columns)

for col in dfFinal2.columns:
        new_col = re.sub(' +', ' ', col).replace("Inflación_Total", "Inflación Total").replace("Inflación_Sin_Alimentos", "Inflación Sin Alimentos")
        dfFinal2 = dfFinal2.rename(columns={col: new_col})

dfFinal2['country'] = 'Colombia'

alphacast.datasets.dataset(329).upload_data_from_df(dfFinal2, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
