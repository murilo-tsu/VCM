print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                                >>  limits.py  <<                                               ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 08/04/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 29/07/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (10/04/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
print('║ - v1.0.1 (30/05/2025): Criação de orientação a objeto para execução de scripts integrados.                     ║')
print('║                                                                                                                ║')
print('║ - v1.0.2 (25/06/2025): Implementando média no arquivo de capacidade de descarga.                               ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Limites de Descarga                                                                                         ║')
print('║ >> Limites de Produção                                                                                         ║')
# print('║ >> Limites de Portos                                                                                           ║')
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
print('Carregando arquivos necessários... \n')

# DataFrame ::  Dicionário Genérico
dicgen = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['dicgen']),
                       sheet_name = arquivos_primarios['dicgen'].split('.')[0],
                       usecols = list(tp_dado_arquivos['dicgen'].keys()),
                       dtype = tp_dado_arquivos['dicgen'])

# DataFrame :: Horizonte (Período) de Otimização
df_periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']),
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])
df_periodos = df_periodos.rename(columns=rename_dataframes['df_periodos'])

# DataFrame :: Capacidade de Produtivas / Armazenagem / Expedição
df_cap_producao = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cap_prod']),
                         sheet_name= arquivos_primarios['cap_prod_sn'], 
                       usecols=list(tp_dado_arquivos['cap_prod'].keys()),
                       dtype=tp_dado_arquivos['cap_prod']).applymap(fx.padronizar)
# (30/07/2025) Criando log de erro para unidades que não foram encontradas no dicgen.
unidades_originais = df_cap_producao.copy()
df_cap_producao['Unidade'] = df_cap_producao['Unidade'].replace(list(dicgen['DE']), list(dicgen['PARA']))
nao_convertidos = unidades_originais[unidades_originais['Unidade'] == df_cap_producao['Unidade']]
nao_convertidos.to_excel(os.path.join(cwd,exec_log_path+'LOG ERROR - Unidades nao encontradas no dicionario generico.xlsx'), sheet_name='Cap_Prod_Armz_Exp', index=False)


# DataFrame :: Capacidade de Armazenagem das Fábricas
df_cap_arm = df_cap_producao.copy()
df_cap_arm_maxmin = df_cap_producao.copy()
# DataFrame :: Capacidade de Descarga das Fábricas
# df_cap_desc = df_cap_producao.copy()

# DataFrame :: Depara Unidades Portuárias :: Porto APO
df_unidades_port = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_por']),
                                 sheet_name = arquivos_primarios['unidades_por_sn'],
                                 usecols=list(tp_dado_arquivos['unidades_por'].keys()),
                                 dtype=tp_dado_arquivos['unidades_por']).applymap(fx.padronizar)

# DataFrame :: Depara Unidades Produtivas / Armazenagem / Expedição
df_unidades = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_exp']),
                         sheet_name= arquivos_primarios['unidades_exp_sn'], 
                       usecols=list(tp_dado_arquivos['unidades_exp'].keys()),
                       dtype=tp_dado_arquivos['unidades_exp']).applymap(fx.padronizar)

# DataFrame :: Capacidade Portos
df_cap_portos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['capacidade_portos']),
                         sheet_name= arquivos_primarios['capacidade_portos_sn'],
                       dtype=tp_dado_arquivos['capacidade_portos']).applymap(fx.padronizar)

# DataFrame :: Template Saída
#fx.validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_saida']))
template_saida = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_saida']),
                       delimiter = ';', encoding = 'utf-8-sig',
                       usecols=list(tp_dado_arquivos['template_saida'].keys()),
                       dtype=tp_dado_arquivos['template_saida'])

# DataFrame :: Template Entrada
#fx.validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_entrada']))
template_entrada = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_entrada']),
                       delimiter = ';', encoding = 'utf-8-sig',
                       usecols=list(tp_dado_arquivos['template_entrada'].keys()),
                       dtype=tp_dado_arquivos['template_entrada'])

# DataFrame :: Template Capacidade
#fx.validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_capacidade']))
# template_capacidade = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_capacidade']),
#                        usecols=list(tp_dado_arquivos['template_capacidade'].keys()),
#                        dtype=tp_dado_arquivos['template_capacidade'])

# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================

print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║  >>  LIMITES DE SAÍDA  <<                                                                                      ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ # Popula a capacidade de expedição das plantas (UP) e dos portos (APO)                                        ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
# PARTE 1 :: LIMITES DE CAPACIDADE DE EXPEDIÇÃO PLANTAS + PORTOS
print('Iniciando preenchimento de limites de expedição')
print('Etapa 01 :: CAPACIDADE DOS PORTOS :: index = APO')

unid_imp = df_unidades[['DEPOSITO','PLANTA','UNIDADE_EXPEDICAO_VCM']]
unid_imp = unid_imp[unid_imp['DEPOSITO'] == '1001'].copy()
df_cap_portos = pd.melt(df_cap_portos, id_vars=['PERIODO'], var_name='Porto', value_name='Capacidade')
df_cap_portos = df_cap_portos.applymap(fx.padronizar)
portos_apo = df_unidades_port[['NOME_AZ_PORTO_VCM','PORTO']].drop_duplicates().dropna(subset = 'NOME_AZ_PORTO_VCM')
df_cap_portos = fx.left_outer_join(df_cap_portos,portos_apo,left_on='Porto', right_on='PORTO',
                                   name_left='Capacidade Portos', name_right='Depara Unidades Portuárias')
df_cap_portos = fx.left_outer_join(df_cap_portos,df_periodos,left_on='PERIODO', right_on='Nome',
                                   name_left='Capacidade Portos', name_right='Períodos')
df_cap_portos = df_cap_portos[['NOME_AZ_PORTO_VCM','Nome VCM','Capacidade']]
df_cap_portos = df_cap_portos.rename(columns={'NOME_AZ_PORTO_VCM':'Unidade','Nome VCM':'Periodo','Capacidade':'Limite'})
df_cap_portos = df_cap_portos.dropna()
df_cap_portos['Limite'] = df_cap_portos['Limite']*1000
print('Etapa 02 :: CAPACIDADE DAS PLANTAS :: index = UP')
df_cap_producao = df_cap_producao[df_cap_producao['Agrupador'] == 'CAPACIDADE PRODUCAO'].copy()
df_cap_producao['DEPOSITO'] = '1001'
df_cap_producao = fx.left_outer_join(df_cap_producao,df_periodos,left_on='Dt/Ref', right_on='Nome',
                                     name_left='Cap. por Unidade', name_right='Períodos')
df_cap_producao = df_cap_producao.dropna()
df_cap_producao['Ativo'] = 'True'
df_cap_producao = fx.left_outer_join(df_cap_producao,df_unidades,left_on=['Unidade','DEPOSITO'], right_on=['PLANTA','DEPOSITO'],name_left='Cap. por Unidade',
                   name_right='Depara de Unidades')
df_cap_producao = df_cap_producao.rename(columns={'Unidade':'Sigla','NOME_VCM':'Unidade','Nome VCM':'Periodo','Quantidade':'Limite'})
template_saida = template_saida.drop(columns={'Limite','Ativo'})
df_cap_producao = df_cap_producao[['UP_MISTURADORA_VCM','Periodo','Limite']].rename(columns={'UP_MISTURADORA_VCM':'Unidade'})
df_cap = pd.concat([df_cap_portos, df_cap_producao])
df_cap['Ativo'] = True
template_saida = fx.left_outer_join(template_saida, df_cap, left_on=['Unidade','Periodo'], right_on=['Unidade','Periodo'],
                   name_left = 'Template Saída', name_right = 'Capacidades Expedição Portos + Unidades')
template_saida = template_saida[['Unidade','Periodo','Limite','Ativo']]
template_saida['Ativo'] = template_saida['Ativo'].fillna('False')
template_saida['Limite'] = template_saida['Limite'].fillna(0.0)
template_saida['Limite'] = template_saida['Limite'].round(2)
template_saida.to_csv(os.path.join(cwd,output_path+'tbOutCapProdPor_LimMaxS.csv'), index = False, sep=';', encoding='utf-8-sig')
print('\nLimites de capacidade de expedição preenchidos!')

print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║  >>  LIMITES DE ENTRADA  <<                                                                                    ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ # Popula a capacidade de descarga das plantas internas (AIN)                                                   ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')

unid_arm = df_unidades[['DEPOSITO','PLANTA','UNIDADE_ARMAZENAGEM_VCM']]
df_cap_arm = df_cap_arm[(df_cap_arm['Agrupador'] == 'CAPACIDADE DESCARGA')&(df_cap_arm['Local'] == 'INTERNO')].copy()
df_cap_arm['Unidade'] = df_cap_arm['Unidade'].replace(list(dicgen['DE']), list(dicgen['PARA']))
# (29/07/2025) Mudando esse merge pois temos as datas de referência.
df_cap_arm = fx.left_outer_join(df_cap_arm, df_periodos, left_on='Dt/Ref', right_on='Nome',
                   name_left = ' Capacidade de Armazenagem das Fábricas', name_right = 'Períodos')
# df_cap_arm = df_cap_arm.merge(df_periodos, how = 'cross')
df_cap_arm['DEPOSITO'] = '1001'
df_cap_arm['Ativo'] = True

# (08/07/2025) Desativando a ideia de média pois o erro estava no merge com períodos, que duplicava as unidades.
# (25/06/2025) Fazendo uma média das quantidades por unidade, para tirar as duplicatas.
# df_cap_arm = df_cap_arm.groupby(by=['Unidade','DEPOSITO','Nome VCM','Ativo'])['Quantidade'].mean().round(2)
# df_cap_arm = df_cap_arm.to_frame()
# df_cap_arm.reset_index(inplace = True)
df_cap_arm = fx.left_outer_join(df_cap_arm, unid_arm, left_on=['Unidade','DEPOSITO'], right_on=['PLANTA','DEPOSITO'],
                   name_left='Unidades de Armazenagem x Depara de Unidades de Armazenagem')
df_cap_arm = df_cap_arm[['UNIDADE_ARMAZENAGEM_VCM','Nome VCM','Quantidade','Ativo']]
df_cap_arm = df_cap_arm.rename(columns={'UNIDADE_ARMAZENAGEM_VCM':'Unidade','Nome VCM':'Periodo','Quantidade':'Limite'})
template_entrada = fx.left_outer_join(template_entrada, df_cap_arm, left_on=['Unidade','Periodo'], right_on=['Unidade','Periodo'],
                   name_left = 'Template Entrada', name_right = 'Capacidade Armazenagem')
template_entrada = template_entrada[['Unidade','Periodo','Limite_y','Ativo_y']]
template_entrada = template_entrada.rename(columns={'Limite_y':'Limite','Ativo_y':'Ativo'})
template_entrada['Ativo'] = template_entrada['Ativo'].fillna('False')
template_entrada['Limite'] = template_entrada['Limite'].fillna(0.0)
template_entrada['Limite'] = template_entrada['Limite'].round(2)
template_entrada.to_csv(os.path.join(cwd,output_path+'tbOutCapDescarga_LimMaxE.csv'), index=False, sep=';', encoding='utf-8-sig')
print('\nLimites de capacidade de descarga preenchidos!')

# (08/07/2025) Como conversado com o Matheus, estou desativando a etapa 
# abaixo no script de limites e levando-a para warehouses.
# print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
# print('║  >>  LIMITES DE CAPACIDADE MÍNIMO E MÁXIMO  <<                                                                 ║')
# print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
# print('║ # Popula a capacidade de armazenagem interno (AIN) e externo (AEX)                                             ║')
# print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')

# # (08/07/2025) Como conversado com o Matheus, retirando a coluna de 
# # data de referência para armazenagem, para evitar problemas.
# df_cap_arm_maxmin = df_cap_arm_maxmin.loc[df_cap_arm_maxmin['Agrupador']=='CAPACIDADE ARMAZENAGEM'].copy()
# df_cap_arm_maxmin['Unidade'] = df_cap_arm_maxmin['Unidade'].replace(list(dicgen['DE']),list(dicgen['PARA']))
# unid_arm['Local'] = np.where(unid_arm['UNIDADE_ARMAZENAGEM_VCM'].str[:3] == 'AIN','INTERNO','EXTERNO')
# unid_arm = unid_arm.drop_duplicates(subset=['PLANTA'])
# df_cap_arm_maxmin = df_cap_arm_maxmin[['Unidade','Quantidade','Local']]
# # (08/07/2025) Desativando a ideia de média.
# # (25/06/2025) Fazendo uma média das quantidades por unidade, para tirar as duplicatas.
# # df_cap_arm_maxmin = df_cap_arm_maxmin.groupby(by=['Unidade','Local'])['Quantidade'].mean().round(2)
# # df_cap_arm_maxmin = df_cap_arm_maxmin.to_frame()
# # df_cap_arm_maxmin.reset_index(inplace = True)
# df_cap_arm_maxmin = fx.left_outer_join(df_cap_arm_maxmin, unid_arm, left_on=['Unidade','Local'], right_on = ['PLANTA','Local'],
#                    name_left='Capacidade de Armazenagem INTERNO e EXTERNO', name_right='Depara de Unidades')
# # (08/07/2025) Adicionando o período como cross, pq cada linha precisa de uma referência de período para o template!
# df_cap_arm_maxmin = df_cap_arm_maxmin.merge(df_periodos, how='cross')
# template_capacidade = fx.left_outer_join(template_capacidade, df_cap_arm_maxmin, left_on=['Unidade','Periodo'], right_on=['UNIDADE_ARMAZENAGEM_VCM','Nome VCM'],
#                    name_left = 'Template Capacidade', name_right = 'Capacidade de Armazenagem INTERNO e EXTERNO')
# template_capacidade['Volume Máximo'] = template_capacidade['Quantidade']
# template_capacidade = template_capacidade[['Unidade_x','Periodo','Volume Mínimo','Volume Máximo']]
# template_capacidade = template_capacidade.rename(columns={'Unidade_x':'Unidade'})
# template_capacidade['Volume Máximo'] =  template_capacidade['Volume Máximo'].fillna(500000)
# template_capacidade.to_excel(os.path.join(cwd,output_path+'tbOutCapacidadeArmazenagem.xlsx'), index=False, sheet_name='VOLUME_AGRUPADO')
end_time = time.time()
print(f'\nTempo de Execução: {round(end_time - start_time,2)} segundos')