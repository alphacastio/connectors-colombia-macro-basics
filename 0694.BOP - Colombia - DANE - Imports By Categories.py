#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import xlrd


from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

#Esto sirve para poder utilizar la libreria xlrd y no genere un problema con python 3.9+
xlrd.xlsx.ensure_elementtree_imported(False, None)
xlrd.xlsx.Element_has_iter = True


# In[3]:


response = requests.get('https://www.dane.gov.co/index.php/estadisticas-por-tema/comercio-internacional/importaciones')


# In[4]:


soup = BeautifulSoup(response.content, 'html.parser')


# In[5]:


#Recopilo los links en una lista
#Cambio la pagina, por lo que se busca por keywords en el nombre del archivo
# links=[]
# for link in soup.find_all('a'):
#     if ('Importaciones mensuales según capítulos del arancel' in link.get_text()) or \
#     ('Importaciones totales según grupos de productos OMC' in link.get_text()):
#         links.append('https://www.dane.gov.co' + link.get('href'))


# In[6]:


#Recopilo los links en una lista
links = []

for link in soup.find_all('a'):
    if (link.get('href') is not None and 'arancel' in link.get('href')) or     (link.get('href') is not None and 'OMC' in link.get('href')):
        links.append('https://www.dane.gov.co' + link.get('href'))


# In[7]:


#Descargo cada uno de los archivos excel
response1 = requests.get(links[0])
response2 = requests.get(links[1])


# In[8]:


###Impo por arancel
#Se usa xlrd porque openpyxl lee ciertos valores como fechas
df0 = pd.read_excel(response1.content, sheet_name=0, skiprows=7, engine='xlrd')


# In[9]:


#Se renombran las variables para facilitar el tratamiento de las columnas
df0.rename(columns={df0.columns[0]:'Categoria', df0.columns[1]:'Mes'}, inplace=True)

df0['Mes'] = df0['Mes'].replace('Total', pd.NA)
df0.dropna(subset=['Mes'], inplace=True)
#Se completa el nombre de la categoria de clasificacion
df0['Categoria'].fillna(method='ffill', inplace=True)


# In[10]:


#Se reemplazan los nombres de los meses con su numeracion
dict_meses={'Enero':'01', 'Febrero':'02', 'Marzo':'03', 'Abril':'04', 'Mayo':'05', 'Junio':'06', 'Julio':'07',
       'Agosto':'08', 'Septiembre':'09', 'Octubre':'10', 'Noviembre':'11', 'Diciembre':'12'}
df0['Mes'] = df0['Mes'].replace(dict_meses)


# In[11]:


#Se hace un stack para que todas las columnas queden en 1 sola
df0 = df0.set_index(['Categoria', 'Mes']).stack().reset_index()


# In[12]:


#Se pivotea para que las categorias queden en columnas y las fechas en el indice
df0 = df0.pivot_table(index=['level_2','Mes'], columns='Categoria', values=0, aggfunc=np.sum).reset_index()


# In[13]:


#Se concatena año y mes, y se transforma a fecha
df0['Date'] = pd.to_datetime(df0['level_2'].astype(str) + '-' + df0['Mes'].astype(str), format='%Y-%m', errors='coerce')


# In[14]:


#Se eliminan las columnas de año y mes, y se fija el indice
df0.drop(['level_2', 'Mes'], axis=1, inplace=True)
df0.set_index('Date', inplace=True)
df0.rename_axis(None, axis=1, inplace=True)


# In[15]:


#####Obtengo el total de las impo
df1 = pd.read_excel(response2.content, sheet_name=0, skiprows=7, engine='xlrd')

#Se eliminan filas con NaN en base al Total
df1.dropna(subset=['Total'], inplace=True)

#Se renombran la columna de la fecha y nos quedamos solo con el Total
df1.rename(columns={'Mes':'Date'}, inplace=True)
df1.set_index('Date', inplace=True)
df1 = df1[['Total']]


# In[16]:


#Se fusionan los dataframes
df = df0.merge(df1, left_index=True, right_index=True, how='outer')
df['country'] = 'Colombia'

alphacast.datasets.dataset(7516).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
