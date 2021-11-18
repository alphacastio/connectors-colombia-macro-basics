#!/usr/bin/env python
# coding: utf-8

# In[18]:


import pandas as pd
import numpy as np
import datetime
import urllib
import time
from urllib.request import urlopen
import requests  


from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


url = "https://www.minhacienda.gov.co/webcenter/ShowProperty?nodeId=%2FConexionContent%2FWCC_CLUSTER-162985%2F%2FidcPrimaryFile&revision=latestreleased"
r = requests.get(url ,allow_redirects=False, verify=False)
df = pd.read_excel(r.content, skiprows= 2)
df = df.iloc[:-2 , 1:].T
df.columns = ["Date", "Sector Público No Financiero (SPNF)", "Sector Público No Financiero (SPNF) - Gobierno General (GG)", "Sector Público No Financiero (SPNF) - Gobierno General (GG) - Gobierno central", "Sector Público No Financiero (SPNF) - Gobierno General (GG) - Gobierno central - Gobierno Nacional Central (GNC)", "Sector Público No Financiero (SPNF) - Gobierno General (GG) - Gobierno central - Resto del CG:", "Sector Público No Financiero (SPNF) - Gobierno General (GG) - Gobierno central - Establecimientos públicos", "Sector Público No Financiero (SPNF) - Gobierno General (GG) - Gobierno central - Del cual, Agencia Nacional de Hidrocarburos (ANH)", "Sector Público No Financiero (SPNF) - Gobierno General (GG) - Gobierno central - Fondo Nacional del Café (FNC)", "Sector Público No Financiero (SPNF) - Gobierno General (GG) - Gobierno central - Fondo de Estabilización de Precios de los Combustibles (FEPC)", "Sector Público No Financiero (SPNF) - Gobierno General (GG) - Gobierno central - Fondo Nacional para el Desarrollo de la Infraestructura (Fondes)*", " Sector Público No Financiero (SPNF) - Gobierno General (GG) - Regionales y locales", "Sector Público No Financiero (SPNF) - Gobierno General (GG) - Regionales y locales - Administraciones centrales de las entidades territoriales (ET)", "Sector Público No Financiero (SPNF) - Gobierno General (GG) - Regionales y locales - Sistema General de Regalías (SGR)", "Sector Público No Financiero (SPNF) - Gobierno General (GG) - Regionales y locales - Fondo de Ahorro y Estabilización Petrolera (FAEP)", " Sector Público No Financiero (SPNF) - Gobierno General (GG) - Seguridad Social", "Sector Público No Financiero (SPNF) - Gobierno General (GG) - Regionales y locales - Salud", "Sector Público No Financiero (SPNF) - Gobierno General (GG) - Regionales y locales - Pensión", " Sector Público No Financiero (SPNF) - Empresas", " Sector Público No Financiero (SPNF) - Empresas - Empresas nacionales", "Sector Público No Financiero (SPNF) - Empresas - Empresas nacionales - Sector eléctrico", "Sector Público No Financiero (SPNF) - Empresas - Empresas nacionales - Ecopetrol", "Sector Público No Financiero (SPNF) - Empresas - Empresas nacionales - Telecom", " Sector Público No Financiero (SPNF) - Empresas - Empresas locales", "Sector Público No Financiero (SPNF) - Empresas - Empresas locales - Empresas Públicas de Medellín (EPM)", "Sector Público No Financiero (SPNF) - Empresas - Empresas locales - Empresa de Telecomunicaciones de Bogotá (ETB)", "Sector Público No Financiero (SPNF) - Empresas - Empresas locales - Metro de Medellín ", "Sector Público No Financiero (SPNF) - Empresas - Empresas locales - Empresa de Acueducto y Alcantarillado de Bogotá (EAAB)", "Sector Público No Financiero (SPNF) - Empresas - Empresas locales - Empresas Municipales de Cali (Emcali)", "Sector Público No Modelado (SPNM)", "Balance primario del SPNF"]
df = df.iloc[1: , :]


# In[20]:


df['Date'] = df['Date'].astype(str) + "-01-01"
df


# In[21]:


df['Date'] = df['Date'].astype(str).str.replace('.0', "", regex=False)
df['Date'] = pd.to_datetime(df['Date'])
df['Date'] = df['Date'].dt.date
df = df.set_index(['Date'])
df["country"] = 'Colombia'

alphacast.datasets.dataset(481).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




