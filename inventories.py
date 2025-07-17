print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                             >>  inventories.py  <<                                             ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 14/03/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 17/07/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (20/03/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
print('║ - v1.0.1 (02/07/2025): Criação de orientação a objeto para execução de scripts integrados                      ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Estoques Contábeis                                                                                          ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
print('\n')

# =======================================================================================================================
# IMPORTAR BIBLIOTECAS
# =======================================================================================================================

import os
import sys
import pandas as pd
import numpy as np
import warnings
import time
import datetime
import logging
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import MonthBegin
from tkinter import messagebox
from unidecode import unidecode
warnings.filterwarnings('ignore')
start_time = time.time()

# =======================================================================================================================
# CONFIGURAÇÕES INICIAIS
# =======================================================================================================================

cwd = os.getcwd()

# Caminhos dos arquivos
structure_path = 'Structural Data/'            # Dados estruturais (topologia)
path = 'Input Data/'                           # Dados de entrada (ciclo de planejamento)
output_path = 'Output Data/'                   # Dados de saída (input para o VCM)
exec_log_path = 'Error Logs/'                  # Logs de erros durante a execução

# Configuração do logger
logging.basicConfig(
    filename=os.path.join(exec_log_path, 'execution_log.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.info("Iniciando execução do script.")

# =======================================================================================================================
# FUNÇÕES
# =======================================================================================================================

from _modulos import aux_functions_vcm
fx = aux_functions_vcm()

# =======================================================================================================================
# DEFINIR ARQUIVOS
# =======================================================================================================================

from _dicionarios import arquivos_primarios, tp_dado_arquivos, rename_dataframes

# =======================================================================================================================
# CARREGAR DATAFRAMES 
# =======================================================================================================================

# DataFRame :: Dicionário Generico
dicgen = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['dicgen']),
                       usecols=list(tp_dado_arquivos['dicgen'].keys()),
                       dtype=tp_dado_arquivos['dicgen']).applymap(fx.padronizar)

# Dataframe :: Template Estoques
#fx.validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_estoques']))
wizard_volumes_iniciais = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_estoques']),
                                        sheet_name = arquivos_primarios['template_estoques_sn'],
                                        usecols = list(tp_dado_arquivos['template_estoques'].keys()),
                                        dtype = tp_dado_arquivos['template_estoques'])

# Dataframe :: Estoques Iniciais
estoques_iniciais = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['estoques_iniciais']),
                                  sheet_name = arquivos_primarios['estoques_iniciais_sn'],
                                  usecols = list(tp_dado_arquivos['estoques_iniciais'].keys()),
                                  dtype = tp_dado_arquivos['estoques_iniciais']).applymap(fx.padronizar)
estoques_iniciais['ACTUAL_STOCK'] = estoques_iniciais['ACTUAL_STOCK'].replace('', pd.NA)
estoques_iniciais['ACTUAL_STOCK'] = estoques_iniciais['ACTUAL_STOCK'].fillna('0')
estoques_iniciais['ACTUAL_STOCK'] = estoques_iniciais['ACTUAL_STOCK'].astype(float)
estoques_iniciais['PRODUCTION_UNIT'] = estoques_iniciais['PRODUCTION_UNIT'].replace(list(dicgen['DE']), list(dicgen['PARA']))
estoques_iniciais['INVOICING_UNIT'] = estoques_iniciais['INVOICING_UNIT'].replace(list(dicgen['DE']), list(dicgen['PARA']))


# Dataframe :: Unidade Armazenagem
depara_armazens = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_exp']),
                                  sheet_name = arquivos_primarios['unidades_exp_sn'],
                                  usecols = list(tp_dado_arquivos['unidades_exp'].keys()),
                                  dtype = tp_dado_arquivos['unidades_exp']).applymap(fx.padronizar)

# Dataframe :: Cadastro Produtos
mp_cadastrada = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['cadastro_produtos_sn01'],
                                  usecols = list(tp_dado_arquivos['cadastro_produtos_sn01'].keys()),
                                  dtype = tp_dado_arquivos['cadastro_produtos_sn01'])

pf_pvo_cadastrada = mp_cadastrada.copy()

# Dataframe :: Agrupamento
mp_estoques = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['cadastro_produtos_sn02'],
                                  usecols = list(tp_dado_arquivos['cadastro_produtos_sn02'].keys()),
                                  dtype = tp_dado_arquivos['cadastro_produtos_sn02'])

mp_estoques = mp_estoques[['COD_ESPECIFICO','CODIGO_AGRUPADO']]


# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================

# Inventários de CTO em ARO 
# Data Mudança: 16/06/2023
estoques_iniciais['Endereco'] = ['TRANSF-CTO' if estoques_iniciais['SUBINVENTORY'][i] == 'TRANSF-CTO'\
                                 else estoques_iniciais['LOCATOR_DESCRIPTION'][i]\
                                 for i in range(estoques_iniciais.shape[0])]

estoques_iniciais = estoques_iniciais.loc[estoques_iniciais.UOM == 'TO']
enderecos = {'ESTOQUE DE QUERENCIA - EM TRANSITO':'QRO',
             'ESTOQUE DE RONDONOPOLIS - NOTA NETA':'RNO',
             'TRANSF-CTO':'CTO',
             # Inventários de CTO em ARO 
             # Antigo: 'ESTOQUE DE CATALAO - EM TRANSITO':'CTO',
             # Novo: 'TRANSF-CTO' : 'CTO'
             'ESTOQUE DE PORTO NACIONAL - EM TRANSITO':'PNO'}

estoques_iniciais = estoques_iniciais.reset_index().drop(columns='index')

for i in range(estoques_iniciais.shape[0]):
    if estoques_iniciais['Endereco'][i] in list(enderecos.keys()):
        estoques_iniciais['INVOICING_UNIT'][i] = enderecos[estoques_iniciais['Endereco'][i]]
estoques_iniciais['ITEM_CODE'] = estoques_iniciais['ITEM_CODE'].str.replace("'","")
estoques_iniciais['CHAVE-ESTOQUE'] = estoques_iniciais['PRODUCTION_UNIT'] + '-' + estoques_iniciais['INVOICING_UNIT'] + '-' + estoques_iniciais['Endereco']
estoques_iniciais = estoques_iniciais[['CHAVE-ESTOQUE','PRODUCTION_UNIT','INVOICING_UNIT','ITEM_CODE','SUBINVENTORY','LOCATOR_DESCRIPTION','ACTUAL_STOCK']]

depara_armazens['CHAVE-ESTOQUE'] = depara_armazens['PLANTA'] + "-" + depara_armazens['PLANTA'] + "-" + depara_armazens['DESCRICAO_DEPOSITO']

estoques_iniciais = estoques_iniciais.merge(depara_armazens, how = 'inner', left_on = 'CHAVE-ESTOQUE', right_on = 'CHAVE-ESTOQUE')

mp_cadastrada = mp_cadastrada.loc[mp_cadastrada.TIPO_MATERIAL.str[:2] == 'MP',:]
pf_pvo_cadastrada = pf_pvo_cadastrada.loc[pf_pvo_cadastrada.TIPO_MATERIAL == 'PF',:]
mp_estoques = mp_estoques.drop_duplicates(subset = ['COD_ESPECIFICO'])
estoques_iniciais = estoques_iniciais.loc[(estoques_iniciais.SUBINVENTORY != 'EMBALAGEM'),:]

# ==============================================================================================================
# (18/03/2025) Ver com o Murilo amanhã se manteremos essa etapa abaixo?

# 13/07/2023 :: Dividir as etapas em estoques de MP e os estoques de PF em Porto Velho
# Porto Velho
estoques_iniciais_pvo = estoques_iniciais.copy()
# Substituir PRODUCTION_UNIT por UNIDADE_ARMAZENAGEM_VCM??
estoques_iniciais_pvo = estoques_iniciais_pvo.loc[estoques_iniciais_pvo.UNIDADE_ARMAZENAGEM_VCM == 'PVO',:]
estoques_iniciais_pvo = estoques_iniciais_pvo.merge(mp_estoques, how = 'left', left_on = 'ITEM_CODE', right_on = 'COD_ESPECIFICO')
estoques_iniciais_pvo = estoques_iniciais_pvo.drop(columns = ['ITEM_CODE','COD_ESPECIFICO']).rename(columns = {'CODIGO_AGRUPADO':'ITEM_CODE'})
estoques_iniciais_pvo = estoques_iniciais_pvo.merge(pf_pvo_cadastrada, how = 'left', right_on = 'CODIGO_ITEM', left_on = 'ITEM_CODE')

# ==============================================================================================================

# Matérias-primas
estoques_iniciais = fx.left_outer_join(estoques_iniciais, mp_estoques, left_on = 'ITEM_CODE', right_on = 'COD_ESPECIFICO',
                                                                               name_left='Estoques Iniciais', name_right='Agrupamento de Produtos')
estoques_iniciais = estoques_iniciais.drop(columns = ['ITEM_CODE','COD_ESPECIFICO']).rename(columns = {'CODIGO_AGRUPADO':'ITEM_CODE'})
estoques_iniciais = fx.left_outer_join(estoques_iniciais, mp_cadastrada, right_on = 'CODIGO_ITEM', left_on = 'ITEM_CODE',
                                       name_left='Estoques Iniciais', name_right='Cadastro de Produtos - MP')

# Utilizar o pd.concat para agrupar os relatórios de PVO e de MPs
estoques_iniciais_agrupados = pd.concat([estoques_iniciais,estoques_iniciais_pvo])

#estoques_iniciais_agrupados = estoques_iniciais_agrupados.dropna()
estoques_iniciais_agrupados = estoques_iniciais_agrupados.groupby(['UNIDADE_ARMAZENAGEM_VCM','PRD-VCM']).agg({'ACTUAL_STOCK':'sum'})
estoques_iniciais_agrupados = estoques_iniciais_agrupados.reset_index()

estoques_iniciais_agrupados['ID'] = estoques_iniciais_agrupados['UNIDADE_ARMAZENAGEM_VCM'] + estoques_iniciais_agrupados['PRD-VCM']
wizard_volumes_iniciais['ID'] = wizard_volumes_iniciais['Unidade'] + wizard_volumes_iniciais['Produto']

wizard_volumes_iniciais = fx.left_outer_join(wizard_volumes_iniciais, estoques_iniciais_agrupados, right_on = 'ID', left_on = 'ID',
                                             name_left='Template Estoques', name_right='Estoques Iniciais Agrupados')
wizard_volumes_iniciais['Valor'] = 0.0
for i in range(wizard_volumes_iniciais.shape[0]):
    if wizard_volumes_iniciais['ACTUAL_STOCK'][i] > 0.0:
        wizard_volumes_iniciais['Valor'][i] = wizard_volumes_iniciais['ACTUAL_STOCK'][i]
wizard_volumes_iniciais = wizard_volumes_iniciais[['Unidade','Produto','Valor']]
wizard_volumes_iniciais['Valor'] = wizard_volumes_iniciais['Valor'].round(2)
wizard_volumes_iniciais.to_excel(os.path.join(cwd,output_path + 'Wizard_Volume_Inicial.xlsx'), sheet_name = 'VOLUME_INICIAL', index = False)
print('Arquivo (Wizard_Volume_Inicial.xlsx) foi Atualizado com Sucesso!\n')

end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')
