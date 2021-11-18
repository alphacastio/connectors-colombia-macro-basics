#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


url = 'https://www.minhacienda.gov.co/webcenter/ShowProperty?nodeId=%2FConexionContent%2FWCC_CLUSTER-128263%2F%2FidcPrimaryFile&revision=latestreleased'

df = pd.read_excel(url,
                  skiprows=range(0,3),
                  nrows=25)

#Elimino la primera columna
df = df.iloc[:, 1:]
#Hago un drop de las filas que tienen todos NaN
df.dropna(axis = 0, how = 'all', inplace=True)
df.reset_index(inplace=True, drop=True)


# In[3]:


#Elimino los espacios en los nombres de los niveles/subniveles
df[df.columns[1]] = df[df.columns[1]].str.strip()


# In[4]:


#Armo una nueva columna en blanco
#donde trae el valor de la columna 1 si no es nulo
#y trae el valor de la fila anterior si en la columna 0 hay un NaN
df['Nivel'] = ""

for i in range(0, df.shape[0]):
    if pd.notnull(df.iloc[i][df.columns[0]]):
        df.at[i, 'Nivel'] = df.iloc[i][df.columns[1]]
    else:
        df.at[i,'Nivel'] = df.at[i-1, 'Nivel']


# In[5]:


#Combino el nombre de las columnas en una nueva columna
df['Nombre'] = ""

#Si el nivel coincide con la columna 1 y el iterador es menor a la cantidad de filas
#Le otorgo el valor de la columna 1
for i in range(0, df.shape[0]):
    if df.iloc[i]['Nivel'] == df.iloc[i][df.columns[1]] and (i < df.shape[0] - 1):
        df.at[i, 'Nombre'] = df.iloc[i][df.columns[1]]
    #Si los valores son diferentes pero no es la última fila, hago un concatenado
    elif df.iloc[i]['Nivel'] != df.iloc[i][df.columns[1]] and (i < df.shape[0] - 1):
        df.at[i,'Nombre'] = df.at[i, 'Nivel'] + ' - ' +df.at[i, df.columns[1]]
    else:
        df.at[i,'Nombre'] = df.at[i, df.columns[1]]


#df['Nombre'] = df[['Nivel' , df.columns[1]]].agg(' - '.join, axis=1)

#Elimino las columnas 0, 1 y la penultima
df.drop([df.columns[0],df.columns[1], df.columns[len(df.columns) - 2]], axis =1 , inplace=True)

#Seteo el indice nuevo previo a transponer
df.set_index('Nombre', inplace =True)
#Traspongo
df = df.T
#Renombro el eje de columnas
df.rename_axis(None,axis=1, inplace=True)


# In[6]:


#Convierto a string
df.index = df.index.map(str)
#Mantengo los primeros 4 caracteres (año)
df.index = df.index.str[:4]

#Cambio el formato a datetime y agrego mes y día
df.index = pd.to_datetime(df.index, format = '%Y').strftime('%Y-%m-%d')

#Cambio el nombre del índice
df.index.names = ['Date']

#Agrego la columna de país
df['country'] = 'Colombia'

alphacast.datasets.dataset(485).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
