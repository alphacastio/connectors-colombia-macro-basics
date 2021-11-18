#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd


from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

response = requests.get('https://www.dane.gov.co/index.php/estadisticas-por-tema/comercio-internacional/exportaciones')
soup = BeautifulSoup(response.content, 'html.parser')


# In[3]:


#Extraigo el link
links = []
for link in soup.find_all('a'):
    #if 'Colombia, exportaciones de café, carbón, petróleo y sus derivados' in link.get_text():
    if link.get('href') is not None and 'cafe' in link.get('href'):
        links.append('https://www.dane.gov.co' + link.get('href'))


# In[4]:


#Descargo el excel
response1 = requests.get(links[0])


# In[5]:


#Leo el excel
df = pd.read_excel(response1.content, sheet_name=0, skiprows=9, engine='openpyxl')
df.set_index(df.columns[0], inplace=True)


# In[6]:


#Elimino las columnas que no tienen cifras en la cuarta fila (corresponde a los montos del primer mes)
#No se puede usar un dropna porque varias filas tienen un 0 en vez de NaN
df = df.loc[:,df.iloc[3] > 0]

df.reset_index(inplace=True)


# In[7]:


#Completo los valores
df.iloc[0] = df.iloc[0].fillna(method='ffill')
df.iloc[1, :-2] = df.iloc[1,:-2].fillna(method='ffill')


# In[8]:


#Hago un list comprehension para concatenar si es tradicional o no
#producto y unidad de medida
columnas = [(df.iloc[0, i] + ' - ' + df.iloc[2, i])             if pd.isna(df.iloc[1, i]) else (df.iloc[0, i] + ' - ' + df.iloc[1, i] + ' - ' + df.iloc[2, i])             for i in range(1, df.shape[1])]
#Como la primera columna queda fuera del list comprehension, la sumo al momento de renombrar las columnas
df.columns=['Date'] + columnas

#Elimino las 3 primeras filas
df = df.iloc[3:, :]
#Paso Date a formato fecha
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
#Elimino los NaN de Date y lo seteo como indice
df.dropna(subset=['Date'], inplace=True)


# In[9]:


df.set_index('Date', inplace=True)
df['country'] = 'Colombia'


alphacast.datasets.dataset(7563).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
