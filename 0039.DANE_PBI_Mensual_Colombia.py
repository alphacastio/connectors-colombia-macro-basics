#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import datetime
import requests
from urllib.request import urlopen
from lxml import etree
import io
from datetime import datetime

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[4]:


# get html from site and write to local file
#url = "https://www.dane.gov.co/index.php/estadisticas-por-tema/cuentas-nacionales/indicador-de-seguimiento-a-la-economia-ise"
#headers = {
#    'Content-Type': 'text/html',
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
#    "Accept-Encoding": "gzip, deflate, br",
#    "Accept-Language": "es-ES,es;q=0.9",
#    "Cache-Control": "no-cache",
#    "Connection": "keep-alive",
#    "Host": "www.dane.gov.co",
#    "Sec-Fetch-Dest": "document",
#    "Sec-Fetch-Mode": "navigate",
#    "Sec-Fetch-Site": "cross-site",
#    "Sec-Fetch-User": "?1",
#    "Upgrade-Insecure-Requests": "1",
#    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36"
#    }
#response = requests.get(url, headers=headers, verify=False)
#html = response.content
#html


# In[5]:


#htmlparser = etree.HTMLParser()
#tree = etree.fromstring(html, htmlparser)
#xls_address = tree.xpath(".//*[@class='table table-hover']/tbody/tr[2]/td[3]/a[1]/@href")[0]
#xls_address


# In[6]:


months = ['dic', 'nov', 'oct', 'sep', 'ago', 'jul', 'jun', 'may', 'abr', 'mar', 'feb', 'ene']
for month in months:
    print('Trying month number:', month)
    url = "https://www.dane.gov.co/files/investigaciones/boletines/pib/Anex_ISE_9actividades_{}_21.xlsx".format(month)
    r = requests.get(url, allow_redirects=True, verify=False)
    try:
        skiprows=9
        cuadros=['Cuadro 1',' Cuadro 2']
        header=[0]

        def get_data(sheet_name):
            with io.BytesIO(r.content) as fh:
                df = pd.read_excel(fh, sheet_name=sheet_name)
            df['Unnamed: 0'][10] = 'month'
            indiceFinal = df[df['Unnamed: 0'] == 'Fuente: DANE, Cuentas nacionales'].index[0]
            df.dropna(axis=0, how='all',inplace=True)
            df.dropna(axis=1, how='all',inplace=True)    
            df = df[:indiceFinal]
            df = df.dropna(how='all', subset= df.columns[1:])
            df = df.set_index('Unnamed: 0')
            df = df.T
            df['Concepto'] = df['Concepto'].fillna(method='ffill')

            df['month'] = df['month'].replace({'Enero':1,
                                            'Febrero':2,
                                          'Marzo':3,
                                          'Abril':4,
                                          'Mayo':5,
                                          'Junio':6,
                                          'Julio':7,
                                          'Agosto':8,
                                          'Septiembre':9,
                                        'Octubre':10,
                                        'Noviembre':11,
                                        'Diciembre':12})

            df['year'] = df['Concepto'].apply(lambda x: str(x).replace('pr','').replace('p',''))
            df["day"] = 1
            df['Date'] = pd.to_datetime(df[["year", "month", "day"]])

            del df['Concepto']
            del df['year']
            del df['day']
            del df['month']    
            df = df.set_index('Date')

            if sheet_name == 'Cuadro 2':
                for col in df.columns:        
                    df = df.rename(columns={col: col + " - sa_orig"})

            return df

        df_final = get_data('Cuadro 1')
        df_final = df_final.merge(get_data('Cuadro 2'), left_index=True, right_index=True)
        
        print('We have a match in month number', month)
        break
    except:
        print('There are no updated data for month', month)
        pass


# In[7]:


df_final = df_final[df_final.columns[3:]]


# In[8]:


df_final


# In[5]:


df_final["country"] = "Colombia"

alphacast.datasets.dataset(39).upload_data_from_df(df_final, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
