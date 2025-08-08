print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                           >>  reposition_cost.py  <<                                           ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 28/04/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 08/08/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (30/04/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
print('║ - v1.0.1 (16/07/2025): Criação de orientação a objeto para execução de scripts integrados.                     ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Custos de Fornecimento de Matérias-Primas                                                                   ║')
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
from tqdm import tqdm
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
print('Carregando arquivos... \n')
print('Tempo de execução esperado: por volta de 3 min \n')

# DataFrame :: Horizonte (Período) de Otimização
df_periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']),
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])

# applymap(fx.padronizar) não aplicado por se tratar de dados com a estrutura final do VCM
df_periodos['pk_NOME_PERIODO'] = df_periodos['NOME_PERIODO'].str.split(' ', expand = True)[0]
id_periodos = df_periodos['NOME_PERIODO'].to_frame()

# Dataframe :: Portos existentes com códigos e correntes.
df_portos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_por']),
                                  sheet_name = arquivos_primarios['unidades_por_sn'],
                                  usecols = list(tp_dado_arquivos['unidades_por'].keys()),
                                  dtype = tp_dado_arquivos['unidades_por']).applymap(fx.padronizar)

# DataFrame :: portos_correntes :: granularidade de correntes
portos_correntes = df_portos.copy()

# DataFrame :: az_portos :: unidades de armazenagem dos portos
az_portos = df_portos.copy()
az_portos = df_portos[['NOME_AZ_PORTO_VCM','PORTO']].drop_duplicates()

# DataFrame :: postos :: apenas a nível de PORTO e NOME_PORTO_VCM
df_portos = df_portos[['NOME_PORTO_VCM','PORTO']].drop_duplicates()

# Dataframe :: Cadastro Produtos
df_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['cadastro_produtos_sn01'],
                                  usecols = list(tp_dado_arquivos['cadastro_produtos_sn01'].keys()),
                                  dtype = tp_dado_arquivos['cadastro_produtos_sn01'])

# DataFrame :: cadastro de matérias-primas :: filtro no tipo de material da tabela CADASTRO
cadastro_mp = df_produtos[(df_produtos['TIPO_MATERIAL'].str.split('-',expand=True)[0].str.strip() == 'MP')]

# Dataframe :: Agrupamento
agrupamento_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['cadastro_produtos_sn02'],
                                  usecols = list(tp_dado_arquivos['cadastro_produtos_sn02'].keys()),
                                  dtype = tp_dado_arquivos['cadastro_produtos_sn02'])
agrupamento_produtos = agrupamento_produtos.loc[agrupamento_produtos['TIPO_MATERIAL']=='MP']
agrupamento_produtos = agrupamento_produtos.drop(columns='TIPO_MATERIAL')
proxy_agrupamento = df_produtos[['CODIGO_ITEM','DESCRICAO']]
proxy_agrupamento = proxy_agrupamento.rename(columns={'CODIGO_ITEM':'COD_ESPECIFICO','DESCRICAO':'DESCRICAO_ESPECIFICA'})
proxy_agrupamento['CODIGO_AGRUPADO'] = proxy_agrupamento['COD_ESPECIFICO']
proxy_agrupamento['AGRUPAMENTO_MP'] = proxy_agrupamento['DESCRICAO_ESPECIFICA']
agrupamento_produtos = pd.concat([agrupamento_produtos,proxy_agrupamento])
agrupamento_produtos = agrupamento_produtos.drop_duplicates(subset = 'COD_ESPECIFICO')

# Dataframe :: Compras importadas :: importa todas as compras firmes IMPORTADAS ou NACIONALIZADAS
df_revisao_importada = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['df_revisao_importada']),
                                  sheet_name = arquivos_primarios['df_revisao_importada'].split('.')[0],
                                  usecols = list(tp_dado_arquivos['df_revisao_importada'].keys()),
                                  dtype = tp_dado_arquivos['df_revisao_importada']).applymap(fx.padronizar)
df_revisao_importada = df_revisao_importada.rename(columns=rename_dataframes['df_revisao_importada'])

# DataFrame :: Compras nacionais :: importa todas as compras firmes NACIONAIS
df_revisao_nacional = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['df_revisao_nacional']),
                                    sheet_name = arquivos_primarios['df_revisao_nacional'].split('.')[0],
                                    usecols = list(tp_dado_arquivos['df_revisao_nacional'].keys()),
                                    dtype = tp_dado_arquivos['df_revisao_nacional']).applymap(fx.padronizar)
df_revisao_nacional = df_revisao_nacional.rename(columns=rename_dataframes['df_revisao_nacional'])

# Dataframe :: Custo de Reposição
custos_mp = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['custo_reposicao']),
                                  sheet_name = arquivos_primarios['custo_reposicao_sn'],
                                  usecols = list(tp_dado_arquivos['custo_reposicao'].keys()),
                                  dtype = tp_dado_arquivos['custo_reposicao'])
custos_mp = custos_mp.loc[custos_mp['CUSTO_REPOSICAO_MERCADO'] > 0.0,:].reset_index().drop(columns='index')

# Dataframe :: Demurrage
demurrage = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['demurrage']),
                                  sheet_name = arquivos_primarios['demurrage_sn'],
                                  dtype = tp_dado_arquivos['demurrage'])
ptax_demurrage = pd.read_excel(os.path.join(path + arquivos_primarios['demurrage']), sheet_name = 'PTAX',
                                  dtype =tp_dado_arquivos['ptax'])

# DataFrame :: Suprimento Intercompany Fornecido por CMISS
suprimento_cmiss =  pd.read_excel(os.path.join(cwd, output_path + arquivos_primarios['demanda_cmiss']),
                                 sheet_name = arquivos_primarios['demanda_cmiss_sn'],
                                 skiprows = 1, usecols = list(tp_dado_arquivos['demanda_cmiss'].keys()),
                                 dtype = tp_dado_arquivos['demanda_cmiss'])

# DataFrame :: Cadastro de Produtos VCM - CMISS
produtos_cmiss = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['produtos_cmiss']),
                               sheet_name = arquivos_primarios['produtos_cmiss_sn01'],
                               usecols = list(tp_dado_arquivos['produtos_cmiss_sn01'].keys()),
                               dtype = tp_dado_arquivos['produtos_cmiss_sn01'])

# Dataframe :: Template Suprimento
fx.validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['wizard_suprimento_faixa']))
template_suprimento = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['wizard_suprimento_faixa']),
                                  usecols = list(tp_dado_arquivos['wizard_suprimento_faixa'].keys()),
                                  dtype = tp_dado_arquivos['wizard_suprimento_faixa'])
wizard_suprimento_faixa = template_suprimento[['Unidade', 'Produto', 'Periodo']]


print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║  >>  PLANO DE COMPRAS  <<                                                                                      ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ # WIZARD_SUPRIMENTO_FAIXA :: Plano de Compras Firmes para VCM                                                  ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
print('Iniciando...')

# 1. Definindo matérias-primas com fornecimento nacional
mp_fornecimento_nacional = cadastro_mp[(cadastro_mp['TIPO_MATERIAL'] == 'MP - COMPRAS')]['PRD-VCM']
mp_fornecimento_nacional = mp_fornecimento_nacional.reset_index().drop(columns='index')
mp_fornecimento_nacional = mp_fornecimento_nacional.rename(columns={'PRD-VCM':'PRD-VCM-NAC'})
mp_fornecimento_nacional = mp_fornecimento_nacional.drop_duplicates()

# 2. Tratando arquivos de plano de compras IMPORTADO e NACIONAL
# 2.1. IMPORTADO
companies = {'FTO':'E600','FH':'E900','SAL':'E890','CMISS':'E890','FHG':'E900','ECFTO':'E600','SFT':'E890'}
df_revisao_importada['COMPANY'] = df_revisao_importada['COMPANY'].replace(companies)
df_revisao_importada = df_revisao_importada[(df_revisao_importada['STATUS'] == 'COMPRADO')]
df_revisao_importada['DT_REMESSA'] = df_revisao_importada['DT_REMESSA'] - pd.offsets.MonthBegin(1)
df_revisao_importada = fx.left_outer_join(df_revisao_importada,agrupamento_produtos,left_on='CODIGO_MP',right_on='COD_ESPECIFICO',
                       name_left='Revisão de Chegadas >>Importada<<', name_right='Agrupamento de Produtos')
df_revisao_importada = fx.left_outer_join(df_revisao_importada, df_periodos, left_on = 'DT_REMESSA', right_on = 'PERIODO',
                       name_left='Revisão de Chegadas >>Importada<<', name_right='Períodos')
df_revisao_importada = fx.left_outer_join(df_revisao_importada, cadastro_mp, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM',
                       name_left='Revisão de Chegadas >>Importada<<', name_right='Cadastro de Produtos VCM')

# 2.2. NACIONAL
id_vars = ['PORTO','PLANTA','MP','STATUS','COMPANY','CODIGO_MP']
df_revisao_nacional = df_revisao_nacional.melt(id_vars = id_vars, var_name = 'PROXY_PERIODO',
                                               value_name = 'BALANCE_TONS')
df_revisao_nacional['COMPANY'] = df_revisao_nacional['COMPANY'].replace(companies)
df_revisao_nacional['PORTO'] = df_revisao_nacional['PORTO'] + '-' + df_revisao_nacional['PLANTA']
df_revisao_nacional = fx.left_outer_join(df_revisao_nacional, agrupamento_produtos, left_on = 'CODIGO_MP', right_on = 'COD_ESPECIFICO',
                      name_left='Revisão de Chegadas >>Nacional<<', name_right='Agrupamento de Produtos')
df_revisao_nacional = fx.left_outer_join(df_revisao_nacional, df_periodos, left_on = 'PROXY_PERIODO', right_on = 'pk_NOME_PERIODO',
                      name_left='Revisão de Chegadas >>Nacional<<',name_right='Períodos')
df_revisao_nacional = fx.left_outer_join(df_revisao_nacional, cadastro_mp, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM',
                      name_left='Revisão de Chegadas >>Nacional<<',name_right='Cadstro de Produtos VCM')

# 3. DataFrame de Compras Completo :: Importado + Nacional
cols = ['PORTO','PLANTA','MP','COMPANY','CODIGO_MP','COD_ESPECIFICO','CODIGO_AGRUPADO','PERIODO','NOME_PERIODO','PRD-VCM','DESCRICAO','TIPO_MATERIAL','CATEGORIA','BALANCE_TONS']
df_revisao_importada = df_revisao_importada[cols]
df_revisao_nacional = df_revisao_nacional[cols]
df_revisao = pd.concat([df_revisao_importada,df_revisao_nacional])
df_revisao = df_revisao.reset_index().drop(columns='index')
df_revisao = fx.left_outer_join(df_revisao, df_portos, left_on = 'PORTO', right_on = 'PORTO',
             name_left='Revisão de Chegadas >>ALL<<', name_right='Portos')

# Salvando um dataframe com o histórico da execução para log_futuro
exec_hist_df_revisao = df_revisao.copy()

# (30/04/2025) :: Linhas acima duplicadas do script supply.py

# CUSTOS DE FORNECIMENTO DE MATÉRIAS-PRIMAS
# =========================================
print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║  Iniciando atualização de Custos de Reposição                                                                  ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
print('\n')

demurrage = demurrage.dropna()
# Tópico 1: Isso ainda é algo a se considerar?
# (17/11/2023: Considerar buscar um PTAX dinâmino na API do BC)
ptax_UF = ptax_demurrage['Cotação (BRL/USD)'][0]

demurrage = demurrage.melt(id_vars = ['Porto','Terminal'],
                           var_name = 'Periodo',
                           value_name = 'Demurrage USD')
demurrage = demurrage.astype({'Periodo':'datetime64[ns]'})
demurrage['Porto'] = demurrage['Porto'].str.replace('(PREMIUM)',' ').str.strip()
demurrage['Demurrage USD'] = demurrage['Demurrage USD'].astype(str)
demurrage['Demurrage USD'] = demurrage['Demurrage USD'].str.replace('$',' ')
demurrage['Demurrage USD'] = demurrage['Demurrage USD'].str.replace(',','.')
demurrage['Demurrage USD'] = demurrage['Demurrage USD'].astype(float)
demurrage['Demurrage BRL'] = demurrage['Demurrage USD'] * ptax_UF
demurrage = demurrage.merge(df_periodos, how = 'left', left_on = 'Periodo', right_on = 'PERIODO')

demurrage = demurrage.merge(df_portos, how = 'left', left_on = 'Porto', right_on = 'PORTO')

demurrage['ID-RIGHT'] = demurrage['NOME_PORTO_VCM'] + '-' + demurrage['NOME_PERIODO']

# (Essas coisas já estavam anotadas no script de FTO)
# Incluir uma coluna denominada DEMURRAGE USD - PREMIUM para replicar os dados de DEMURRAGE USD
# Renomear o nome dos portos para remover o nível de premium
# Criar um dataframe agrupado
demurrage['Demurrage BRL - PREMIUM'] = demurrage['Demurrage BRL']
demurrage = demurrage.groupby(['Porto','Terminal','Periodo','ID-RIGHT']).agg({'Demurrage BRL':'min','Demurrage BRL - PREMIUM':'max'})
demurrage = demurrage.reset_index()
custos_mp = custos_mp.rename(columns = {'DT_INICIAL':'Data Inicial','DT_FINAL':'Data Final'})
custos_mp = custos_mp.merge(df_periodos[['PERIODO','NOME_PERIODO']], how = 'cross')

# Caso haja período do VCM sem valor, usa o LAST_UPDATED_COST
last_updated_cost = custos_mp.copy()
last_updated_cost = last_updated_cost[['PERIODO','CD_PRODUTO','CODIGO_MOEDA','CUSTO_REPOSICAO_MERCADO']]
last_updated_cost = last_updated_cost.sort_values(by = 'PERIODO', ascending = False)
last_updated_cost = last_updated_cost.reset_index().drop(columns = 'index')
last_updated_cost['Custo VCM (BRL/ton)'] = np.nan
for i in range(last_updated_cost.shape[0]):
    if last_updated_cost['CODIGO_MOEDA'][i] == 'USD':
        last_updated_cost['Custo VCM (BRL/ton)'][i] = last_updated_cost['CUSTO_REPOSICAO_MERCADO'][i] * ptax_UF
    else:
        last_updated_cost['Custo VCM (BRL/ton)'][i] = last_updated_cost['CUSTO_REPOSICAO_MERCADO'][i]
last_updated_cost = last_updated_cost.drop_duplicates(subset=['CD_PRODUTO'], keep = 'first')

# (11/02/2025) Olhando primeiro o código específico, depois o agrupado.
# Utilizando o "Agrupamento" do cadastro de produtos.
agrupamento_produtos = agrupamento_produtos.drop_duplicates(subset=['COD_ESPECIFICO'])
agrupamento_produtos = fx.left_outer_join(agrupamento_produtos, cadastro_mp, right_on = 'CODIGO_ITEM', left_on = 'CODIGO_AGRUPADO',
                                          name_left='Agrupamento de Produtos', name_right='Cadastro de Produtos - MP')
last_updated_cost = fx.left_outer_join(last_updated_cost, agrupamento_produtos, right_on = 'COD_ESPECIFICO', left_on = 'CD_PRODUTO',
                                       name_left='Último Custo Atualizado', name_right='Agrupamento de Produtos')
# last_updated_cost['COD_ESPECIFICO'] = last_updated_cost['COD_ESPECIFICO'].fillna('0')
# agrupamento_custo = last_updated_cost.loc[last_updated_cost['COD_ESPECIFICO']=='0']
# agrupamento_custo = agrupamento_custo[['CD_PRODUTO','Custo VCM (BRL/ton)']]
# agrupamento_custo = fx.left_outer_join(agrupamento_custo, agrupamento_produtos, right_on = 'CODIGO_AGRUPADO', left_on = 'CD_PRODUTO',)
# agrupamento_custo['CODIGO_AGRUPADO'] = agrupamento_custo['CODIGO_AGRUPADO'].fillna('0')
# agrupamento_custo = agrupamento_custo.loc[agrupamento_custo['CODIGO_AGRUPADO']!='0']
# last_updated_cost = last_updated_cost.loc[last_updated_cost['CODIGO_AGRUPADO']!='0']
# last_updated_cost = pd.concat([last_updated_cost,agrupamento_custo])
last_updated_cost = last_updated_cost[['Custo VCM (BRL/ton)','PRD-VCM','CD_PRODUTO']]
last_updated_cost = last_updated_cost.dropna().reset_index().drop(columns = 'index')
last_updated_cost = last_updated_cost[['PRD-VCM','Custo VCM (BRL/ton)']].rename(columns = {'Custo VCM (BRL/ton)':'LAST_UPDATED_COST','PRD-VCM':'ID'})

# (11/02/2025) Olhando primeiro o código específico, depois o agrupado.
custos_mp = fx.left_outer_join(custos_mp, agrupamento_produtos, right_on = 'COD_ESPECIFICO', left_on = 'CD_PRODUTO',
                                                  name_left='Custo de Reposição', name_right='Agrupamento de Produtos')
# tbDadoPrimarioCustoReposicao['COD_ESPECIFICO'] = tbDadoPrimarioCustoReposicao['COD_ESPECIFICO'].fillna('0')
# agrupamento_custo = tbDadoPrimarioCustoReposicao.loc[tbDadoPrimarioCustoReposicao['COD_ESPECIFICO']=='0']
# agrupamento_custo = agrupamento_custo[['DH_VIGOR','DH_REFERENCIA', 'Data Inicial', 'Data Final', 'CD_PRODUTO','DESCRICAO_ITEM',
#                                     'CODIGO_ORGANIZACAO','CODIGO_MOEDA','PTAX_DIA_ANTERIOR','CUSTO_REPOSICAO_MERCADO','PERIODO','NOME_PERIODO']]
# agrupamento_custo = fx.left_outer_join(agrupamento_custo, agrupamento_produtos, right_on = 'CODIGO_AGRUPADO', left_on = 'CD_PRODUTO',)
# agrupamento_custo['CODIGO_AGRUPADO'] = agrupamento_custo['CODIGO_AGRUPADO'].fillna('0')
# agrupamento_custo = agrupamento_custo.loc[agrupamento_custo['CODIGO_AGRUPADO']!='0']
# tbDadoPrimarioCustoReposicao = tbDadoPrimarioCustoReposicao.loc[tbDadoPrimarioCustoReposicao['COD_ESPECIFICO']!='0']
# custos_mp = pd.concat([tbDadoPrimarioCustoReposicao, agrupamento_custo])

# Criar regra para estabelecer períodos
# Utilizar LAST_UPDATED_COST caso False
custos_mp['Validar'] = (custos_mp['PERIODO'] >= custos_mp['Data Inicial']) & (custos_mp['PERIODO'] <= custos_mp['Data Final'])
custos_mp = custos_mp.loc[custos_mp.Validar == True]
custos_mp = custos_mp.reset_index().drop(columns = ['index','Validar','Data Inicial','Data Final'])
custos_mp = fx.left_outer_join(custos_mp, agrupamento_produtos, right_on = 'COD_ESPECIFICO', left_on = 'CD_PRODUTO',)
custos_mp['Custo VCM (BRL/ton)'] = np.nan
for i in range(custos_mp.shape[0]):
    if custos_mp['CODIGO_MOEDA'][i] == 'USD':
        custos_mp['Custo VCM (BRL/ton)'][i] = custos_mp['CUSTO_REPOSICAO_MERCADO'][i] * ptax_UF
    else:
        custos_mp['Custo VCM (BRL/ton)'][i] = custos_mp['CUSTO_REPOSICAO_MERCADO'][i]

# O custo de reposição é dado pela média dos custos de reposição do mercado
# Portanto, a unidade para qual está sendo comprado não faz diferença
custos_mp = custos_mp.groupby(by=['NOME_PERIODO','PRD-VCM_x'])['Custo VCM (BRL/ton)'].mean().reset_index()
custos_mp = custos_mp.merge(last_updated_cost, how = 'left', left_on = 'PRD-VCM_x', right_on = 'ID')
custos_mp = custos_mp.rename(columns={'PRD-VCM_x':'PRD-VCM'})
# (12/02/2025) Fazendo uma média para o last_updated_cost, já que temos o mesmo MP,
# mas códigos diferentes, então temos custos diferentes para cada código.
custos_mp = custos_mp.groupby(by=['NOME_PERIODO','PRD-VCM','Custo VCM (BRL/ton)'])['LAST_UPDATED_COST'].mean().reset_index()
wizard_custo_suprimento_faixa = template_suprimento.drop(columns = ['Suprimento Mínimo', 'Suprimento Máximo'])
wizard_custo_suprimento_faixa['ID-LEFT'] = wizard_custo_suprimento_faixa['Unidade'] + '-' + wizard_custo_suprimento_faixa['Periodo']

# (04/07/2025) Retirando duplicatas por PRD-VCM, pois isso estava alterando a estrutura do template.
agrupamento_produtos = agrupamento_produtos.drop_duplicates(subset=['PRD-VCM'])

demurrage = fx.left_outer_join(demurrage, df_periodos, left_on = 'Periodo', right_on = 'PERIODO',
                               name_left='Demurrage', name_right='Períodos')
demurrage = fx.left_outer_join(demurrage, df_portos, left_on = 'Porto', right_on = 'PORTO',
                               name_left='Demurrage', name_right='Portos')
demurrage['ID-RIGHT'] = demurrage['NOME_PORTO_VCM'] + '-' + demurrage['NOME_PERIODO']
wizard_custo_suprimento_faixa['Custo MP BRL/ton'] = 0.0
wizard_custo_suprimento_faixa['Demurrage BRL/ton'] = 0.0
wizard_custo_suprimento_faixa = fx.left_outer_join(wizard_custo_suprimento_faixa, agrupamento_produtos[['PRD-VCM','CATEGORIA']],
                                                   left_on='Produto',right_on='PRD-VCM',)
wizard_custo_suprimento_faixa['Validação'] = '0'
# ===============================================================================================================================
# (29/01/2025) Excluindo produtos que existem no template mas não no Dado Primario do Custo de Reposição
produtos_existentes = custos_mp.loc[custos_mp['LAST_UPDATED_COST']!=0.0]
produtos_existentes = produtos_existentes['PRD-VCM'].to_frame()
produtos_existentes = produtos_existentes.rename(columns={'PRD-VCM':'PRD-VCM1'})
produtos_existentes = produtos_existentes.drop_duplicates()
wizard_custo_suprimento_faixa = wizard_custo_suprimento_faixa.merge(produtos_existentes, how='left',
                                                                left_on='PRD-VCM', right_on='PRD-VCM1', indicator=True)
wizard_custo_suprimento_faixa = wizard_custo_suprimento_faixa.loc[wizard_custo_suprimento_faixa['_merge']=='both'].reset_index()
wizard_custo_suprimento_faixa = wizard_custo_suprimento_faixa.drop(columns={'PRD-VCM1','_merge','index'})
# ===============================================================================================================================

print('Realizando preenchimentos de custos de acordo com as datas de vigência...')
for i in tqdm(range(wizard_custo_suprimento_faixa.shape[0])):
    for j in range(custos_mp.shape[0]):
        if  wizard_custo_suprimento_faixa['Produto'][i] == custos_mp['PRD-VCM'][j] and wizard_custo_suprimento_faixa['Periodo'][i] == custos_mp['NOME_PERIODO'][j]:
            wizard_custo_suprimento_faixa['Custo MP BRL/ton'][i] = custos_mp['Custo VCM (BRL/ton)'][j]
            wizard_custo_suprimento_faixa['Validação'][i] = '1'
            
        # Regra adicional para buscar o LAST_UPDATED_COST
        elif wizard_custo_suprimento_faixa['Validação'][i] == '0' and wizard_custo_suprimento_faixa['Produto'][i] == custos_mp['PRD-VCM'][j]:
            wizard_custo_suprimento_faixa['Custo MP BRL/ton'][i] = custos_mp['LAST_UPDATED_COST'][j]
            wizard_custo_suprimento_faixa['Validação'][i] = '1'  
                      
    for k in range(demurrage.shape[0]):
        if wizard_custo_suprimento_faixa['Validação'][i] != '0' and wizard_custo_suprimento_faixa['ID-LEFT'][i] == demurrage['ID-RIGHT'][k]:
            if wizard_custo_suprimento_faixa['CATEGORIA'][i] == 'PREMIUM':
                wizard_custo_suprimento_faixa['Demurrage BRL/ton'][i] = demurrage['Demurrage BRL - PREMIUM'][k]
            if wizard_custo_suprimento_faixa['CATEGORIA'][i] == 'CONVENCIONAL':
                wizard_custo_suprimento_faixa['Demurrage BRL/ton'][i] = demurrage['Demurrage BRL'][k]

wizard_custo_suprimento_faixa['Custo do Produto'] = wizard_custo_suprimento_faixa['Custo MP BRL/ton'] + wizard_custo_suprimento_faixa['Demurrage BRL/ton']
wizard_custo_suprimento_faixa = wizard_custo_suprimento_faixa[['Unidade','Produto','Periodo','Custo do Produto']]
wizard_suprimento_faixa = fx.left_outer_join(wizard_suprimento_faixa, wizard_custo_suprimento_faixa, left_on = ['Unidade','Produto','Periodo'],
                                             right_on = ['Unidade','Produto','Periodo'], name_left='Template Suprimento Faixa', name_right='Wizard de Custos')
wizard_suprimento_faixa = wizard_suprimento_faixa.fillna(0.0)
wizard_suprimento_faixa['Custo do Produto'] = wizard_suprimento_faixa['Custo do Produto'].round(2)
wizard_suprimento_faixa.to_excel(os.path.join(cwd,output_path + 'tbOutCustosFornecCFR.xlsx'), sheet_name = 'CUSTO_PRODUTO', index = False)
print('Arquivo (tbOutCustosFornecCFR.xlsx) foi Atualizado com Sucesso!')
end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')