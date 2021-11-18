#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests

from datetime import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
import io
import re


from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


#Se descarga el archivo de Anexos 

url_gral = 'https://www.dane.gov.co/index.php/estadisticas-por-tema/mercado-laboral/empleo-y-desempleo'
page = requests.get(url_gral,verify=False)
soup = BeautifulSoup(page.content, 'html.parser')


# In[3]:


link_xls = []

for link in soup.find_all('a'):
    if link.get('title') is not None and 'Anexo' in link.get('title') and 'RELAB' not in link.get('href'):
        link_xls.append(link.get('href'))


# In[4]:


url1 = 'https://www.dane.gov.co/' + link_xls[0]
r = requests.get(url1, allow_redirects=True, verify=False)
df = pd.read_excel(r.content,skiprows=11,sheet_name='Tnal mensual')

df = df.dropna(how='all').dropna(how='all', subset = df.columns[1:])

df.columns = df.columns.astype(str)
df.columns = pd.Series([np.nan if 'Unnamed:' in x else x for x in df.columns.values]).ffill().values.flatten()

df['Concepto'] = df['Concepto'].fillna('')

df["level_1"] = df["Concepto"]

sublevels = {
    'level_2':["  Insuficiencia de horas", "  Empleo inadecuado por competencias", "  Empleo inadecuado por ingresos"],
}
        
for sublevel in sublevels:
    df[sublevel] = np.nan
    print(sublevel)
    df.loc[df["level_1"].isin(sublevels[sublevel]), sublevel] = df["level_1"]
    df.loc[df["level_1"].isin(sublevels[sublevel]), "level_1"] = np.nan

for x in range(len(sublevels), 0,  -1):
    print(x)
    df["level_{}_temp".format(x)] = df["level_{}".format(x)].ffill()
    df.loc[df["level_{}".format(x)].notnull(), "level_{}".format(x)] = df["level_{}_temp".format(x)]

df["concept"] = df["level_1_temp"]
for y in [2]:
    df.loc[df["level_{}".format(y)].notnull(), "concept"] = df["concept"] + " - " + df["level_{}".format(y)]
#     print(y)

df = df.drop(['level_1','level_2','level_1_temp','Concepto'],axis=1)

df = df.rename(columns = {'concept': 'Concepto'})
df = df.set_index("Concepto")
df = df.transpose()
df = df.rename(columns = {'': 'Mes'})
df = df.reset_index()

df['Mes'] = df['Mes'].str.replace("*", "").replace(
        {
        "Ene": "01-01",
        "Feb": "02-01",
        "Mar": "03-01",
        "Abr": "04-01",
        "May": "05-01",
        "Jun": "06-01",
        "Jul": "07-01",
        "Ago": "08-01",
        "Sep": "09-01",
        "Oct": "10-01",
        "Nov": "11-01",
        "Dic": "12-01",

        })

df['Date'] = df['index'] + '-' + df['Mes']
df['Date'] = df['Date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
del df['index']
del df['Mes']
df = df.set_index('Date')

newCols=[]
for col in df.columns:
    newCols += [col+' - '+'Total nacional']        
df.columns = newCols
# df


# In[5]:


#Se descarga el de Series desestacionalizadas
link_xls = []


for link in soup.find_all('a'):
    if link.get('title') is not None and 'Series desestacionalizadas' in link.get('title'):
        link_xls.append(link.get('href'))


# In[6]:


url2 = 'https://www.dane.gov.co/' + link_xls[0]
r = requests.get(url2, allow_redirects=True, verify=False)


# In[7]:


sheets=['Tnal mensual','Mensual sexo']

#Serie desestacionalizada - Total nacional
df1 = pd.read_excel(r.content,skiprows=11,sheet_name=sheets[0])
df1 = df1.dropna(how='all').dropna(how='all', subset = df1.columns[1:])

df1.columns = df1.columns.astype(str)
df1.columns = pd.Series([np.nan if 'Unnamed:' in x else x for x in df1.columns.values]).ffill().values.flatten()
df1 = df1.set_index('Concepto')
df1 = df1.T
df1 = df1.rename(columns={np.nan: 'Mes'})
df1 = df1.reset_index()
df1 = df1.rename(columns={'index':'Año'})

df1['Mes'] = df1['Mes'].str.replace("*", "").replace(
        {
        "Ene": "01-01",
        "Feb": "02-01",
        "Mar": "03-01",
        "Abr": "04-01",
        "May": "05-01",
        "Jun": "06-01",
        "Jul": "07-01",
        "Ago": "08-01",
        "Sep": "09-01",
        "Oct": "10-01",
        "Nov": "11-01",
        "Dic": "12-01",

        })

df1['Date'] = df1['Año'] + '-' + df1['Mes']
df1['Date'] = df1['Date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
del df1['Año']
del df1['Mes']

df1 = df1.set_index('Date')

newCols=[]
for col in df1.columns:
    newCols += [col+' - '+'Total nacional desestacionalizado']        
df1.columns = newCols

#Serie desestacionalizada - Hombres
df2 = pd.read_excel(r.content,skiprows=12,sheet_name=sheets[1])
df2 = df2.dropna(how='all').dropna(how='all', subset = df2.columns[1:])

df2.columns = df2.columns.astype(str)
df2.columns = pd.Series([np.nan if 'Unnamed:' in x else x for x in df2.columns.values]).ffill().values.flatten()
df2 = df2.loc[0:7, :]

df2 = df2.set_index('Concepto')
df2 = df2.T
df2 = df2.reset_index()

df2 = df2.rename(columns={np.nan: 'Mes'})
df2 = df2.rename(columns={'index':'Año'})

df2['Mes'] = df2['Mes'].str.replace("*", "").replace(
        {
        "Ene": "01-01",
        "Feb": "02-01",
        "Mar": "03-01",
        "Abr": "04-01",
        "May": "05-01",
        "Jun": "06-01",
        "Jul": "07-01",
        "Ago": "08-01",
        "Sep": "09-01",
        "Oct": "10-01",
        "Nov": "11-01",
        "Dic": "12-01",

        })

df2['Date'] = df2['Año'] + '-' + df2['Mes']
df2['Date'] = df2['Date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
del df2['Año']
del df2['Mes']

df2 = df2.set_index('Date')

newCols=[]
for col in df2.columns:
    newCols += [col+' - '+'Hombres']        
df2.columns = newCols

#Serie desestacionalizada - Mujeres
df3 = pd.read_excel(r.content,skiprows=27,sheet_name=sheets[1])
df3 = df3.dropna(how='all').dropna(how='all', subset = df3.columns[1:])

df3.columns = df3.columns.astype(str)
df3.columns = pd.Series([np.nan if 'Unnamed:' in x else x for x in df3.columns.values]).ffill().values.flatten()

df3 = df3.set_index('Concepto')
df3 = df3.T
df3 = df3.reset_index()

df3 = df3.rename(columns={np.nan: 'Mes'})
df3 = df3.rename(columns={'index':'Año'})

df3['Mes'] = df3['Mes'].str.replace("*", "").replace(
        {
        "Ene": "01-01",
        "Feb": "02-01",
        "Mar": "03-01",
        "Abr": "04-01",
        "May": "05-01",
        "Jun": "06-01",
        "Jul": "07-01",
        "Ago": "08-01",
        "Sep": "09-01",
        "Oct": "10-01",
        "Nov": "11-01",
        "Dic": "12-01",

        })

df3['Date'] = df3['Año'] + '-' + df3['Mes']
df3['Date'] = df3['Date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
del df3['Año']
del df3['Mes']

df3 = df3.set_index('Date')

newCols=[]
for col in df3.columns:
    newCols += [col+' - '+'Mujeres']        
df3.columns = newCols

df_desest = df1.merge(df2, how='outer', left_index=True, right_index=True).merge(df3, how='outer', left_index=True, right_index=True)

dfFinal = df.merge(df_desest, how='outer', left_index=True, right_index=True)

dfFinal["country"] = "Colombia"

alphacast.datasets.dataset(315).upload_data_from_df(dfFinal, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



