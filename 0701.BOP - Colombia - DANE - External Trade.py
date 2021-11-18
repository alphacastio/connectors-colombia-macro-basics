#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from functools import reduce


from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

#Expo
response0 = requests.get('https://www.dane.gov.co/index.php/estadisticas-por-tema/comercio-internacional/exportaciones')
soup0 = BeautifulSoup(response0.content, 'html.parser')

#Impo
response1 = requests.get('https://www.dane.gov.co/index.php/estadisticas-por-tema/comercio-internacional/importaciones')
soup1 = BeautifulSoup(response1.content, 'html.parser')

#Balanza
response2 = requests.get('https://www.dane.gov.co/index.php/estadisticas-por-tema/comercio-internacional/balanza-comercial')
soup2 = BeautifulSoup(response2.content, 'html.parser')


# In[3]:


#Obtenemos los links para cada archivo y luego se procesa de manera separada
#Primero expos, luego impos y al final el de balanza comercial
link_xls_expo = []
for link in soup0.find_all('a'):
    #Cambio la página pero se mantiene esta línea en el script por si llega a cambiar nuevamente
    #if 'Colombia, exportaciones totales según grupos de productos OMC' in link.get_text():
    if link.get('href') is not None and 'OMC' in link.get('href'):
        link_xls_expo.append('https://www.dane.gov.co' + link.get('href'))


# In[4]:


link_xls_impo = []
for link in soup1.find_all('a'):
    #if 'Importaciones totales según grupos de productos OMC' in link.get_text():
    if link.get('href') is not None and 'OMC' in link.get('href'):
        link_xls_impo.append('https://www.dane.gov.co' + link.get('href'))


# In[5]:


link_xls_balanza = []
for link in soup2.find_all('a'):
#     if 'Balanza comercial - serie mensual' in link.get_text():
    if link.get('title') is not None and 'Balanza comercial - serie mensual' in link.get('title'):
        link_xls_balanza.append('https://www.dane.gov.co' + link.get('href'))


# In[6]:


link_xls_balanza


# In[7]:


response_expo = requests.get(link_xls_expo[0])
response_impo = requests.get(link_xls_impo[0])
response_balanza = requests.get(link_xls_balanza[0])


# In[8]:


###############################
####Exportaciones###############
###############################
df_expo = pd.read_excel(response_expo.content, sheet_name=0, skiprows=10, engine='openpyxl')


# In[9]:


#Renombro todas las columnas para eliminar el numero al final
df_expo.rename(columns={'Mes':'Date',
                       'Agropecuarios, alimentos y bebidas1': 'Agropecuarios, alimentos y bebidas',
                       'Combustibles y Prod de industrias extractivas 2': 'Combustibles y Prod de industrias extractivas',
                       'Manufacturas3': 'Manufacturas',
                       'Otros sectores 4': 'Otros sectores'}, inplace=True)


# In[10]:


#Paso a formato fecha y elimino filas sin fecha
df_expo['Date'] = pd.to_datetime(df_expo['Date'], errors='coerce')
df_expo.dropna(subset=['Date'], inplace=True)


# In[11]:


df_expo.set_index('Date', inplace=True)
df_expo = df_expo.add_prefix('Exportaciones - ')


# In[12]:


###############################
####Importaciones##############
###############################
df_impo = pd.read_excel(response_impo.content, sheet_name=0, skiprows=8)


# In[13]:


#Renombro todas las columnas para eliminar el numero al final
df_impo.rename(columns={df_impo.columns[0]:'Date',
                       df_impo.columns[1]:'Total',
                       'Agropecuarios, alimentos y bebidas1':'Agropecuarios, alimentos y bebidas',
                       'Combustibles y Prod de industrias extractivas 2':'Combustibles y Prod de industrias extractivas',
                       'Manufacturas3':'Manufacturas',
                       'Otros sectores 4': 'Otros sectores'}, inplace=True)

#Paso a formato fecha y elimino filas sin fecha
df_impo['Date'] = pd.to_datetime(df_impo['Date'], errors='coerce')
df_impo.dropna(subset=['Date'], inplace=True)


# In[14]:


df_impo.set_index('Date', inplace=True)
df_impo = df_impo.add_prefix('Importaciones - ')
df_impo = df_impo.add_suffix(' - CIF')


# In[15]:


###############################
###Balanza Comercial###########
###############################
df_balanza = pd.read_excel(response_balanza.content, sheet_name=0, skiprows=8)


# In[16]:


#Renombro las columnas, paso a formato fecha
df_balanza.rename(columns={df_balanza.columns[0]:'Date'}, inplace=True)
df_balanza['Date'] = pd.to_datetime(df_balanza['Date'], errors='coerce')
#Elimino las filas que no tengan fecha
df_balanza.dropna(subset=['Date'], inplace=True)
#Elimino la columna Expo porque ya está. La de impo se mantiene porque está en FOB y en el otro archivo está en CIF
df_balanza.drop('Exportaciones', axis=1, inplace=True)
df_balanza.set_index('Date', inplace=True)

#Multiplico los valores por 1000 para que todos queden en miles
df_balanza = df_balanza*1000


# In[17]:


data_frames = [df_expo, df_impo, df_balanza]


# In[18]:


#Junto todos los dataframes
df = reduce(lambda  left,right: pd.merge(left,right,left_index=True, right_index=True,
                                            how='outer'), data_frames)

df['country'] = 'Colombia'

alphacast.datasets.dataset(7575).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



