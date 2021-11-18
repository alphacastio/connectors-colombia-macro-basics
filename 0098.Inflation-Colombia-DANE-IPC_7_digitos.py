#!/usr/bin/env python
# coding: utf-8

# In[10]:


#!pip install fuzzywuzzy


# In[2]:


import pandas as pd
import requests
import io

from tqdm import tqdm

import warnings
warnings.filterwarnings('ignore')
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[3]:


meses = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic']
anos = ['19', '20','21']


# In[4]:


def remove_accents(x):
    x = x.replace('á', 'a')
    x = x.replace('é', 'e')
    x = x.replace('í', 'i')
    x = x.replace('ó', 'o')
    x = x.replace('ú', 'u')
    return x


# In[5]:


ipc_col_df = pd.DataFrame([])
for ano in tqdm(anos, desc='Year progress'):
    for mes in tqdm(meses, desc='Month progress'):
        url = 'https://www.dane.gov.co/files/investigaciones/boletines/ipc/anexo_ipc_{}{}.xlsx'.format(mes,ano)
        
        sheetname = '8'
        skiprows= 7
        r = requests.get(url, allow_redirects=False, verify=False)
        
        if r.status_code == 200:
#             print(url)
            try:
                with io.BytesIO(r.content) as datafr:
                    df = pd.read_excel(datafr, skiprows = skiprows, sheet_name = sheetname)

                df = df[['Subclase', 'Índice']][1:]
            except:
                with io.BytesIO(r.content) as datafr:
                    df = pd.read_excel(datafr, skiprows = skiprows, sheet_name = '7')

                df = df[['Subclase', 'Índice']][1:]
                
            df = df.dropna()
            df['Subclase'] = df['Subclase'].apply(lambda x: x.capitalize())
            
            def replace_values(x):
                if "Bebidas calientes : tinto," in x:
                    x = 'Bebidas calientes : tinto, café con leche, chocolate, té, bebida achocolatada caliente, leche, agua de panela, agua aromática, avena caliente y similares  para consumo inmediato, en establecimientos de servicio a la mesa y autoservicio.'
                elif "Gaseosa y otros refrescos" in x:
                    x= 'Gaseosa y otros refrescos  en establecimientos de servicio a la mesa y autoservicio,  medios de transporte, maquinas expendedoras, puestos moviles, y lugares de esparcimiento; se incluyen tambien las adquiriudas para llevar y servicio a domicilio'
                elif 'Transporte intermunicipal' in x:
                    x= 'Transporte intermunicipal, interveredal e internacional'
                elif 'Whisky' in x:
                    x= 'Whisky, ron, brandy, vodka, ginebra, coñac, tequila, cremas de licor y aperitivos'
                elif 'Frutas frescas' in x:
                    x = 'Frutas Frescas'
                elif 'Hortalizas y legumbres frescas' in x:
                    x = 'Hortalizas Y Legumbres Frescas'
                elif "Arriendo efectivo" in x:
                    x= "Arriendo efectivo"
                elif 'Suscripción y servicio de' in x:
                    x= 'Suscripción y servicio de televisión por redes y cable'
                elif 'Comidas en establecimientos' in x:
                    x= 'Comidas en establecimientos de servicio a la mesa y autoservicio,  medios de transporte, máquinas expendedoras, puestos móviles, y lugares de esparcimiento; se incluyen también las  contratadas por encargo, para llevar y por servicio a domicilio'
                elif 'Gasto en servicios de la vivienda ocupada' in x:
                    x='Arriendo imputado'
                return x
            
            df['Subclase'] = df['Subclase'].apply(lambda x: replace_values(x))
            df['Subclase'] = df['Subclase'].apply(lambda x: remove_accents(x))
            df['Subclase'] = df['Subclase'].apply(lambda x: x.replace('  ', ' '))
            df = df.set_index('Subclase')

            df = df.T

            def date_maker(link):
                date = link.split('_')[-1].split('.')[0]
                x = date[:3]
                year = '20'+ date[3:]
                if x == 'ene':
                    x = '1-1'
                elif x == 'feb':
                    x = '2-1'
                elif x == 'mar':
                    x = '3-1'
                elif x == 'abr':
                    x = '4-1'
                elif x == 'may':
                    x = '5-1'
                elif x == 'jun':
                    x = '6-1'
                elif x == 'jul':
                    x = '7-1'
                elif x == 'ago':
                    x = '8-1'
                elif x == 'sep':
                    x = '9-1'
                elif x == 'oct':
                    x = '10-1'
                elif x == 'nov':
                    x = '11-1'
                elif x == 'dic':
                    x = '12-1'

                final_date = year+'-'+x
                return final_date

            df['Date'] = date_maker(url)
                
            ipc_col_df = ipc_col_df.append(df)


ipc_col_df = ipc_col_df.set_index('Date')
ipc_col_df['country'] = 'Colombia'

alphacast.datasets.dataset(98).upload_data_from_df(ipc_col_df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



