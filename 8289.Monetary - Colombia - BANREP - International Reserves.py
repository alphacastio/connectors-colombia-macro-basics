#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests_html
import pandas as pd
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


# In[3]:


#Se utiliza requests_html y Session porque permite cargar los elementos de javascript
#Con el requests comun no funciona
session = requests_html.HTMLSession ()


# In[4]:


#Descargo el archivo. El archivo trae el historico
f_path = '%2Fshared%2FSeries%20Estad%C3%ADsticas_T%2F1.%20Reservas%20internacionales%2F1.1%20Serie%20hist%C3%B3rica%20-%20IQY'
response = session.get('https://totoro.banrep.gov.co/analytics/saw.dll?Download&Format=excel2007&Extension=.xls'
                       '&BypassCache=true&lang=es&path='
                       f'{f_path}'
                       '&NQUser=publico&NQPassword=publico123&SyncOperation=1')


# In[5]:


df = pd.read_excel(response.content)


# In[6]:


#Elimino las filas con NaNs
df.dropna(inplace=True)

#Cambio el nombre de las columnas y elimino la primera fila
df.columns = ['Estado', 'Date','Reservas brutas', 'Reservas netas', 
              'Reservas brutas (sin FLAR)', 'Reservas netas (sin FLAR)']
df = df.iloc[1:]


# In[7]:


#Elimino la primera columna
df.drop('Estado', axis=1, inplace=True)

#Convierto a formato fecha. Como siempre toma el ultimo día, lo reemplazo por el día 1 en todos los meses
df['Date'] = pd.to_datetime(df['Date'])
df['Date'] = df['Date'].apply(lambda dt: dt.replace(day=1))


# In[8]:


df.set_index('Date', inplace=True)
df['country'] = 'Colombia'

#Para el primer envio se cargo todo, despues se envían los últimos 12 meses
df = df.tail(12)


# In[9]:


# dataset_name = 'Monetary - Colombia - BANREP - International Reserves'

##Para raw data
# dataset = alphacast.datasets.create(dataset_name, 965, 0)

# alphacast.datasets.dataset(dataset['id']).initialize_columns(dateColumnName = 'Date', 
#             entitiesColumnNames=['country'], dateFormat= '%Y-%m-%d')


# In[10]:


alphacast.datasets.dataset(8289).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




