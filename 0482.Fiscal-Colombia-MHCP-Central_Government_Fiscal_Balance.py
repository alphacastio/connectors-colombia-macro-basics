#!/usr/bin/env python
# coding: utf-8

# In[8]:


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

url = "https://www.minhacienda.gov.co/webcenter/ShowProperty?nodeId=%2FConexionContent%2FWCC_CLUSTER-161521%2F%2FidcPrimaryFile&revision=latestreleased"
r = requests.get(url ,allow_redirects=False, verify=False)
df = pd.read_excel(r.content, skiprows= 3).dropna(how='all')
df = df.iloc[:-7 , :].drop(1)
df = df.drop("Unnamed: 0", axis=1).T
df


# In[14]:


df.columns = ["Date" , "Ingresos Totales" , "Ingresos Totales - Ingresos Corrientes de la Nación" , "Ingresos Totales - Ingresos Corrientes de la Nación - Ingresos Tributarios" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Renta" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Cuotas" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Retención" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - IVA interno" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - IVA externo" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Gravamen arancelario" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Sobretasa a la Importaciones CIF" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Imp. Nacional a la Gasolina y ACPM" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Impuesto al Carbono" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Consumo" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - CREE" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Sobretasa a la Gasolina y ACPM" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Gravamen movimientos financieros" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Resto" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Timbre" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Impuesto SIMPLE" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Normalización" , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Retención en la fuente inmuebles " , "Ingresos Totales - Ingresos Corrientes de la Nación - DIAN - Contribución para la Democracia (Patrimonio) / Impuesto a la riqueza" , "Ingresos Totales - Ingresos Corrientes de la Nación - Ingresos No Tributarios " , "Ingresos Totales - Ingresos Corrientes de la Nación - Ingresos No Tributarios - Contribución de Hidrocarburos" , "Ingresos Totales - Ingresos Corrientes de la Nación - Ingresos No Tributarios - Concesiones" , "Ingresos Totales - Ingresos Corrientes de la Nación - Ingresos No Tributarios -  Telefonía Celular" , "Ingresos Totales - Ingresos Corrientes de la Nación - Ingresos No Tributarios - Concesiones portuarias y otros" , "Ingresos Totales - Ingresos Corrientes de la Nación - Ingresos No Tributarios - Resto" , "Ingresos Totales - Fondos Especiales" , "Ingresos Totales  - Otros recursos de capital" , "Ingresos Totales - Otros Recursos de Capital - Rendimientos Financieros Totales" , "Ingresos Totales - Otros Recursos de Capital -  Excedentes Financieros" , "Ingresos Totales - Otros Recursos de Capital - Ecopetrol" , "Ingresos Totales - Otros Recursos de Capital - Banco de la República" , "Ingresos Totales - Otros Recursos de Capital - Telecom" , "Ingresos Totales - Otros Recursos de Capital - Isa e Isagen" , "Ingresos Totales - Otros Recursos de Capital - Bancóldex" , "Ingresos Totales - Otros Recursos de Capital - Estapúblicos" , "Ingresos Totales - Otros Recursos de Capital - Resto de empresas" , "Ingresos Totales - Otros Recursos de Capital - Recuperación de cartera diferente SPNF" , "Ingresos Totales - Otros Recursos de Capital - Otros recursos" , "Ingresos Totales - Otros Recursos de Capital - Reintegros y recursos no apropiados" , "Ingresos Totales - Otros Recursos de Capital - Resto" , "Pagos Totales" , "Pagos Totales sin intereses" , "Pagos Totales - Pagos Corrientes de la Nación " , "Pagos Totales - Pagos Corrientes de la Nación - Intereses" , "Pagos Totales - Pagos Corrientes de la Nación - Intereses deuda externa" , "Pagos Totales - Pagos Corrientes de la Nación - Intereses deuda interna" , "Pagos Totales - Pagos Corrientes de la Nación - Costo impuesto endeudamiento externo" , "Pagos Totales - Pagos Corrientes de la Nación - Funcionamiento" , "Pagos Totales - Pagos Corrientes de la Nación - Servicios personales" , "Pagos Totales - Pagos Corrientes de la Nación - Transferencias" , "Pagos Totales - Pagos Corrientes de la Nación - Transferencias regionales (SGP desde 2002)" , "Pagos Totales - Pagos Corrientes de la Nación - Situado Fiscal" , "Pagos Totales - Pagos Corrientes de la Nación - Participaciones municipales" , "Pagos Totales - Pagos Corrientes de la Nación - Fondo de Compensación Educativa" , "Pagos Totales - Pagos Corrientes de la Nación - Pensiones" , "Pagos Totales - Pagos Corrientes de la Nación - Otras transferencias" , "Pagos Totales - Pagos Corrientes de la Nación - Gastos generales y otros" , "Pagos Totales - Inversión" , "Déficit o Superávit Efectivo" , "Déficit o Superávit Efectivo - PRESTAMO NETO           " , "Déficit o Superávit Efectivo - INGRESOS CAUSADOS" , "BORRAR" , "Déficit o Superávit Efectivo - GASTOS CAUSADOS" , "BORRAR" , "Déficit o Superávit Efectivo - DEUDA FLOTANTE" , "Déficit o Superávit Total" , "Costos de la reest. Financiera " , "Costos de la reest. Financiera - Capitalización Intereses Fogafín" , "Costos de la reest. Financiera - Indexación Tes Ley 546" , "Costos de la reest. Financiera - Indexación TRD" , "Costos de la reest. Financiera - Intereses Ley 546" , "" , "Costos de la reest. Financiera - Amortización TRD" , "Costos de la reest. Financiera - Amortiz. Ley 456" , "Costos de la reest. Financiera - Liquidación Caja Agraria  " , "Déficit a Financiar" , "Balance Primario"]
df = df.iloc[1: , :]


# In[15]:


df['Date'] = df['Date'].astype(str) + "-01-01"


# In[16]:


df['Date'] = df['Date'].astype(str).str.replace('.0', "", regex=False)
df['Date'] = pd.to_datetime(df['Date'])
df['Date'] = df['Date'].dt.date
df = df.set_index(['Date'])
df["country"] = 'Colombia'
df = df.loc[:,~df.columns.duplicated()]


del df["BORRAR"]

alphacast.datasets.dataset(482).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
