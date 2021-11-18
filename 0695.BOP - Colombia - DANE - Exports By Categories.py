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


# In[3]:


soup = BeautifulSoup(response.content, 'html.parser')


# In[4]:


links = []
for link in soup.find_all('a'):
    #Removieron el texto del link, se cambia el script pero se mantiene la línea por si cambia nuevamente
    #if 'Colombia, exportaciones según capítulos del arancel' in link.get_text():
    if link.get('href') is not None and 'aranc' in link.get('href'):
        links.append('https://www.dane.gov.co' + link.get('href'))


# In[5]:


response1 = requests.get(links[0])


# In[6]:


df = pd.read_excel(response1.content, sheet_name=0, skiprows=9, engine='openpyxl')
#Renombro las columnas
df.rename(columns={df.columns[0]:'Date', df.columns[-1]:'Total exportaciones'}, inplace=True)
#Convertimos a formato fecha
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df.dropna(subset=['Date'], inplace=True)


# In[7]:


df.set_index('Date', inplace=True)

df['country'] = 'Colombia'

alphacast.datasets.dataset(7522).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




