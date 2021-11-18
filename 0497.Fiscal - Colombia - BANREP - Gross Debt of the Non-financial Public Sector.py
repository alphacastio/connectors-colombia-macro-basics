#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd

import requests

from bs4 import BeautifulSoup

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

url_banrep = 'https://www.banrep.gov.co/es/boletin-deuda-publica'

#descargo el contenido de la pagina
banrep_html = requests.get(url_banrep).text

soup = BeautifulSoup(banrep_html, 'html.parser')


# In[3]:


#Extraigo los links de todos los boletines
links = soup.find_all('a', href=True)

#Filtro los links en base al texto
texto = 'Boletín con información'
#Armo una lista con todos los links
links_xls = []

#Itero sobre los links y los agrego a la lista
for link in links:
    if texto in link.text:
        links_xls.append(link.get('href'))


# In[4]:


#Hoja -C1A-C1B-
df1 = pd.read_excel(links_xls[0], 
                    sheet_name=2,
                   skiprows=2)
                   #usecols=range(3,17))
#Hago una copia del dataframe
df2 = df1.copy()

#Mantengo solo unas columnas
df1 = df1.iloc[:,3:17]


# In[5]:


#Genero una variable para asignar el numero de la ultima fila
last_row = ''

#Hago un loop para determinar cual es la fila con datos y si las 2 siguientes están vacías
for i in range(4, df1.shape[0]):
    if pd.notnull(df1.iloc[i][df1.columns[0]]) and pd.isna(df1.iloc[i+1][df1.columns[0]]) and pd.isna(df1.iloc[i+2][df1.columns[0]]):
        last_row = i
        break

#Reduzco las dimensiones del dataframe en base al limite obtenido (2 filas vacias)
df1 = df1[:last_row + 1]

#Elimino las columnas sin valores (columnas intermedias)
df1.dropna(how='all', axis=1, inplace = True)
#Elimino las filas vacías
df1.dropna(how='all', axis=0, inplace = True)


# In[6]:


#Asigno el mismo título/nivel en todas las celdas
df1.iloc[0, 1:10] = df1.iloc[0,0]

#Reemplazo manual de parentesis y asterisco
df1.replace('\(', '', regex=True, inplace=True)
df1.replace('\)', '', regex=True, inplace=True)
df1.replace('\*', '', regex=True, inplace=True)


# In[7]:


#Hago un loop para completar la fila 2

for i in range(1, df1.shape[1]):
    if pd.isna(df1.iloc[2,i]):
        df1.iloc[2,i] = df1.iloc[2, i -1]


# In[8]:


#Hago un merge de titulos
for i in range(1, df1.shape[1]):
    if pd.notnull(df1.iloc[0,i]):
        df1.iloc[0,i] = df1.iloc[0, i] + " - " + df1.iloc[1, i] + " - " + df1.iloc[2, i]
    else:
        df1.iloc[0,i] = df1.iloc[1, i] + " - " + df1.iloc[2, i]

#Cambio el nombre de las columnas
df1.columns = df1.iloc[0]
#Hago un drop de varias filas
df1.drop(df1.index[0:3], axis=0, inplace=True)

#Reemplazo el índice
df1.set_index(df1.columns[0], inplace=True)

#Renombro el axis de columnas
df1.rename_axis(None,axis=1, inplace=True)


# In[9]:


#Renombro el indice
df1.index.names = ['Date']
#Cambio el formato de fecha del índice
df1.index = df1.index.strftime('%Y-%m-%d')


# In[10]:


#Tomo el valor de last_row para poder ver donde termina la otra tabla y luego comenzar con el nuevo dataframe
df2 = df2.iloc[last_row + 4:, 1:]

#Elimino Columnas con todos NaN
df2.dropna(how='all', axis=1, inplace = True)
#Elimino filas con todos NaN
df2.dropna(how='all', axis=0, inplace = True)


# In[11]:


#Reduzco la cantidad de filas para eliminar las últimas que están ocultas
#Genero una variable para asignar el numero de la ultima fila
last_row2 = ''

#Hago un loop para determinar cual es la fila con datos y si las 2 siguientes están vacías
for i in range(4, df2.shape[0]):
    if pd.notnull(df2.iloc[i][df2.columns[1]]) and pd.isna(df2.iloc[i+1][df2.columns[1]]) and pd.isna(df2.iloc[i+2][df2.columns[1]]):
        last_row2 = i
        break

#Reduzco las dimensiones del dataframe en base al limite obtenido (2 filas vacias)
df2 = df2[:last_row2+ 1]


# In[12]:


#Asigno el mismo título/nivel en todas las celdas
df2.iloc[0, 1:] = df2.iloc[0,0]

#Reemplazo manual de parentesis y asterisco
df2.replace('\(', '', regex=True, inplace=True)
df2.replace('\)', '', regex=True, inplace=True)
df2.replace('\*', '', regex=True, inplace=True)


# In[13]:


#Hago un doble loop para completar las filas subsiguientes
for j in range(1, 4):
    for i in range(1, df2.shape[1]):
        if pd.isna(df2.iloc[j,i]):
            df2.iloc[j,i] = df2.iloc[j, i -1]
            
            
#Hago un merge de titulos
for i in range(1, df2.shape[1]):
    df2.iloc[0,i] = df2.iloc[0, i] + " - " + df2.iloc[1, i] + " - " + df2.iloc[2, i] + " - " + df2.iloc[3, i]


# In[14]:


#Cambio el nombre de las columnas
df2.columns = df2.iloc[0]
#Hago un drop de varias filas
df2.drop(df2.index[0:4], axis=0, inplace=True)

#Reemplazo el índice
df2.set_index(df2.columns[0], inplace=True)

#Renombro el axis de columnas
df2.rename_axis(None,axis=1, inplace=True)

#Renombro el indice
df2.index.names = ['Date']
#Cambio el formato de fecha del índice
df2.index = df2.index.strftime('%Y-%m-%d')


# In[15]:


#Hago un left join
df = df1.merge(df2, left_index=True, right_index=True, how='left')
#Agrego el pais en una columna nueva
df['country'] = 'Colombia'
#Ordeno las filas en base al indice
df.sort_index(inplace=True)

alphacast.datasets.dataset(497).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
