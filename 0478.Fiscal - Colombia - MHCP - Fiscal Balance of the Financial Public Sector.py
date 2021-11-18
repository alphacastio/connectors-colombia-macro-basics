#!/usr/bin/env python
# coding: utf-8

# In[14]:


#Cargo las librerias
import pandas as pd
import requests

from bs4 import BeautifulSoup


from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


#Para resolver el problema del certificado al levantar el excel de una url
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


# In[16]:


url_hacienda='https://www.minhacienda.gov.co/webcenter/portal/EntidadesFinancieras/pages_EntidadesFinancieras/PoliticaFiscal/bgg/balancefiscalsectorpblicofinanciero'


# In[17]:


#descargo el contenido de la pagina
hacienda_html = requests.get(url_hacienda, verify=False).text


# In[18]:


soup = BeautifulSoup(hacienda_html, 'html.parser')


# In[19]:


#Extraigo el link
links = soup.find_all('a', href=True)
url_base = 'https://www.minhacienda.gov.co/'
texto = 'Sector Público Financiero 2001'
link_xls = ''
for link in links:
    if texto in link.text:
        link_xls = link.get('href')


# In[20]:


#Leo la url, elimino las primeras filas y traspongo
df = pd.read_excel(link_xls, 
                   skiprows=range(0,3),
                   nrows=3,sheet_name=0).T


# In[21]:


#Mantengo a partir de la segunda fila
df = df.iloc[2: , :]


# In[22]:


#Cambio el formato del índice
df.index = pd.to_datetime(df.index, format = '%Y')
df.index = df.index.strftime('%Y-%m-%d')


# In[23]:


#Nombres de las variables
columnas = ["Sector Público Financiero", 
            "Sector Público Financiero - Banco de la República", 
           "Sector Público Financiero - Fogafín"]
df.columns = columnas
#Cambio el nombre del índice
df.index.names = ['Date']
#Agrego la columna correspondiente al país
df['country'] = 'Colombia'


# In[24]:


df.index.names = ['Date']

alphacast.datasets.dataset(478).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)




