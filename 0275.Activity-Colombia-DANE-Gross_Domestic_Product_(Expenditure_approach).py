#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from datetime import datetime
import requests
import numpy as np
import re


from bs4 import BeautifulSoup

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


#Fijo la url a utilizar
url_dane = 'https://www.dane.gov.co/index.php/estadisticas-por-tema/cuentas-nacionales/cuentas-nacionales-trimestrales/pib-informacion-tecnica'

#descargo el contenido de la pagina
dane_html = requests.get(url_dane, verify=False).text

soup = BeautifulSoup(dane_html, 'html.parser')


# In[3]:


#Extraigo los links de todos los boletines
links = soup.find_all('a', href=True)

texto = 'Consultar Anexos estadísticos de gasto'
#Armo una lista con todos los links a descargar (precios corrientes y constantes)
files = []

#Itero sobre los links y los agrego a la lista
for link in links:
    #Verifico que el titulo del link no sea nulo y que contenga el texto definido
    if link.get('title') is not None and texto in link.get('title'):
        files.append(link.get('href'))


# In[4]:


url = 'https://www.dane.gov.co/'
sheets = ['Cuadro 1', 'Cuadro 2']

dfFinal = pd.DataFrame([])
i=0


# In[5]:


for file in files:
    r = requests.get(url+file, allow_redirects=False, verify=False)
    for sheet in sheets:
        df=pd.read_excel(r.content, sheet_name=sheet, skiprows=9)
        df = df.reset_index(drop=True)
        end = df[df[df.columns[0]] == 'Fuente: DANE, Cuentas nacionales '].index[0]
        df = df[:end]
        df = df.dropna(how='all')
        df = df[df.columns[1:]]
        boldtitle = ''
        df[df.columns[0]] = df[df.columns[0]].astype(str)
        for value in range(0,df.shape[0]):
            if len(df.iloc[value,0])<4:
                boldtitle = df[df.columns[0]][value]
            elif (len(df.iloc[value,0])>3) & (df.iloc[value,1] != 'Producto interno bruto'):
                pass
            else:
                df.iloc[value,0] = boldtitle + ' - '+ df.iloc[value,0]
        df[df.columns[0]] = df[df.columns[0]].fillna('')
        df['Concepto'] = df[df.columns[0]] + ' - ' + df['Concepto']
        #df = df[df.columns[1:]]
        df = df.set_index('Concepto')

        df = df.T
        df = df.reset_index()
        df['index'] = df['index'].apply(lambda x: np.nan if ('Unnamed' in str(x)) else x)

        df['index'] = df['index'].fillna(method='ffill')
        df['index'] = df['index'].apply(lambda x: str(x).replace('pr','').replace('p',''))

        def replaceQuarter(x):
            x=str(x)
            if x == 'I':
                x='01-01'
            elif x == 'II':
                x='04-01'
            elif x == 'III':
                x='07-01'
            elif x == 'IV':
                x='10-01'
            else:
                x=np.nan
            return x

        df = df.reset_index(drop=True)

        df['Date']=df[df.columns[1]].apply(lambda x: replaceQuarter(x))

        df['Date'] = (df['index'].astype(str)+'-'+df['Date']).apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))
        del df[df.columns[1]]
        del df['index']
        
        df = df.set_index('Date')
        
        newCols = []
        for col in df.columns:
            col = re.sub(r"(-){2,}", "-", col)
            if col[:3] == ' - ':
                    col = col[3:]
            
            newCols+=[col]
        
        df.columns = newCols
        
        colname = []
        for col in df.columns:
            if (sheet == 'Cuadro 2') & (file==files[0]):
                col += ' - sa_orig'
            elif (sheet == 'Cuadro 2') & (file==files[1]):
                col += ' - real_sa_orig'
            elif (sheet == 'Cuadro 1') & (file==files[0]):
                col += ' - constant_prices_orig'
            colname +=[col]
            
        df.columns = colname
        
        if i==0:
            dfFinal = df
        else:
            dfFinal = dfFinal.merge(df, how='outer', left_index=True, right_index=True)
        
        i+=1
dfFinal


# In[6]:


dfFinal['country'] = 'Colombia'

alphacast.datasets.dataset(275).upload_data_from_df(dfFinal, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




