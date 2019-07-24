import google.datalab.storage as storage
import pandas as pd
import sys
import simplejson as json
import numpy as np
import csv
import re
from io import BytesIO

#--------------------------------------------------------------------------Buscando o arquivo no Cloud PlatForm----------------------------------------------------------------------------------------

mybucket = storage.Bucket('fca-finance-origin') #fca-finance
# data = mybucket.object('Conciliacao/EXTRACAO_FIASA_CBU_GAAP_VB_OPT_20190704014650.txt')
data = mybucket.object('Conciliacao/EXTRACAO_FIASA_CBU_GAAP_VB_OPT_novo.txt')

##cadastro = storage.Object.read_lines(data,max_lines=100)
cadastro = storage.Object.read_stream(data,start_offset=0, byte_count=None)

destbucket = storage.Bucket('fca-finance') #fca-finance
blob_file = destbucket.object('OTC_Bonus_Incentivos/tabFiasa2/Extracao Fiasa.tsv')

print (cadastro[:1000])

#-------------------------------------------------------------------------Mostrando os dados em uma Lista---------------------------------------------------------------------------------------
try:
    tamanho = len(cadastro)
    print ("\n ARQUIVO TABULADO: ")
    
    print (type(cadastro))    
    print (cadastro [:1000])
    
    arquivo = cadastro.decode("utf-8").replace('|', '\t').encode('utf-8')

#     texto = str(arquivo).strip('[]')
   
    output = BytesIO()
    out = output.write(arquivo)
    print (output.getvalue()[:100000])

    # ------------------------------------------------------------------Gerar o arquivo no FCA-FINANCE--------------------------------------------------------------
    
    blob_file.write_stream(output.getvalue(), 'text/plain')
    
except TypeError:
    print ("Ocorreu um erro ao gerar o arquivo .tsv" )
    
    # # write_stream("teste",  'text/plain:
    # # blob_file.upload(arquivo, 'text/plain')
    # blob_file.upload(arquivo)
