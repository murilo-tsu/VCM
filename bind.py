print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                                >>  bind.py  <<                                                 ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 14/05/2025                                                ║')
print('║ Editado por:   Murilo Lima Ribeiro             Data: 25/08/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v2.0.0 (25/08/2025): Release Projeto Merger                                                                  ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Amarração das Correntes de Fornecimento                                                                     ║')
print('║ >> Amarração das Correntes de Demanda                                                                          ║')
print('║ >> As amarrações definem limites forçados nas correntes quando informados, reduzindo os graus de liberdade     ║')
print('║    do problema de otimização.                                                                                  ║')
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
from tqdm import tqdm
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
print('Carregando arquivos necessários... \n')

# DataFrame :: Horizonte (Período) de Otimização
df_periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']),
                            sheet_name=arquivos_primarios['periodos_sn'],
                            usecols=list(tp_dado_arquivos['periodos'].keys()),
                            dtype=tp_dado_arquivos['periodos'])
# applymap(padronizar) não aplicado por se tratar de dados com a estrutura final do VCM
df_periodos['pk_NOME_PERIODO'] = df_periodos['NOME_PERIODO'].str.split(' ', expand = True)[0]
id_periodos = df_periodos['NOME_PERIODO'].to_frame()

# DataFrame :: Portos
df_portos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['portos']),
                            sheet_name= arquivos_primarios['portos'].split('.')[0], 
                            usecols=list(tp_dado_arquivos['portos'].keys()),
                            dtype=tp_dado_arquivos['portos']).applymap(fx.padronizar)
id_portos = df_portos.drop(columns=['PORTO']).drop_duplicates()
# DataFrame :: postos :: apenas a nível de PORTO e NOME_PORTO_VCM
df_portos = df_portos[['NOME_PORTO_VCM','PORTO']].drop_duplicates()

# DataFrame :: Correntes
df_correntes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['portos']),
                            sheet_name= arquivos_primarios['portos'].split('.')[0], 
                            usecols=list(tp_dado_arquivos['portos_correntes'].keys()),
                            dtype=tp_dado_arquivos['portos_correntes']).applymap(fx.padronizar)

# DataFrame :: Update Correntes
dep_correntes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['correntes']),
                            sheet_name= arquivos_primarios['correntes_sn'], 
                            usecols=list(tp_dado_arquivos['correntes'].keys()),
                            dtype=tp_dado_arquivos['correntes']).applymap(fx.padronizar)

# DataFrame :: Cadastro de Produtos
df_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                            sheet_name = arquivos_primarios['cadastro_produtos_sn01'],
                            usecols = list(tp_dado_arquivos['cadastro_produtos_sn01'].keys()),
                            dtype = tp_dado_arquivos['cadastro_produtos_sn01'])

# (19/08/2025) Filtrando os códigos que não (~) começam com 'MP' ou 'PF' para pegar apenas produtos que existem de fato.
cadastro_produtos = df_produtos.loc[~df_produtos['CODIGO_ITEM'].str.startswith(('MP', 'PF'))]

# DataFrame :: cadastro de matérias-primas :: filtro no tipo de material da tabela CADASTRO
cadastro_mp = df_produtos[(df_produtos['TIPO_MATERIAL'].str.split('-',expand=True)[0].str.strip() == 'MP')]

# DataFrame :: cadastro de produto final :: filtro no tipo de material da tabela CADASTRO
pf_cadastrada = df_produtos[(df_produtos['TIPO_MATERIAL'].str.split('-',expand=True)[0].str.strip() == 'PF')]

# DataFramse :: Agrupamento
df_agrupamento = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                            sheet_name = arquivos_primarios['cadastro_produtos_sn02'],
                            usecols = list(tp_dado_arquivos['cadastro_produtos_sn02'].keys()),
                            dtype = tp_dado_arquivos['cadastro_produtos_sn02'])

# Agrupamento de Produtos Acabados
agrupamento_produtos_pf = df_agrupamento.copy()
agrupamento_produtos_pf = agrupamento_produtos_pf[agrupamento_produtos_pf['TIPO_MATERIAL'] == 'PF']
agrupamento_produtos_pf = agrupamento_produtos_pf.drop(columns='TIPO_MATERIAL')
proxy_agrupamento_pf = pf_cadastrada[['CODIGO_ITEM','DESCRICAO']]
proxy_agrupamento_pf = proxy_agrupamento_pf.rename(columns={'CODIGO_ITEM':'COD_ESPECIFICO','DESCRICAO':'DESCRICAO_ESPECIFICA'})
proxy_agrupamento_pf['CODIGO_AGRUPADO'] = proxy_agrupamento_pf['COD_ESPECIFICO']
proxy_agrupamento_pf['AGRUPAMENTO'] = proxy_agrupamento_pf['DESCRICAO_ESPECIFICA']
agrupamento_produtos_pf = pd.concat([agrupamento_produtos_pf,proxy_agrupamento_pf])
agrupamento_produtos_pf = agrupamento_produtos_pf.drop_duplicates(subset = 'COD_ESPECIFICO')

# Agrupamento de Matérias-Primas
agrupamento_produtos_mp = df_agrupamento.copy()
agrupamento_produtos_mp = agrupamento_produtos_mp[agrupamento_produtos_mp['TIPO_MATERIAL'] == 'MP']
agrupamento_produtos_mp = agrupamento_produtos_mp.drop(columns='TIPO_MATERIAL')
proxy_agrupamento_mp = cadastro_mp[['CODIGO_ITEM','DESCRICAO']]
proxy_agrupamento_mp = proxy_agrupamento_mp.rename(columns={'CODIGO_ITEM':'COD_ESPECIFICO','DESCRICAO':'DESCRICAO_ESPECIFICA'})
proxy_agrupamento_mp['CODIGO_AGRUPADO'] = proxy_agrupamento_mp['COD_ESPECIFICO']
proxy_agrupamento_mp['AGRUPAMENTO'] = proxy_agrupamento_mp['DESCRICAO_ESPECIFICA']
agrupamento_produtos_mp = pd.concat([agrupamento_produtos_mp,proxy_agrupamento_mp])
agrupamento_produtos_mp = agrupamento_produtos_mp.drop_duplicates(subset = 'COD_ESPECIFICO')

# DataFrame :: Compras Importadas
df_revisao_importada = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['df_revisao_importada']),
                            sheet_name =arquivos_primarios['df_revisao_importada'].split('.')[0],
                            usecols = list(tp_dado_arquivos['df_revisao_importada'].keys()),
                            dtype = tp_dado_arquivos['df_revisao_importada']).applymap(fx.padronizar)

df_revisao_importada = df_revisao_importada.rename(columns=rename_dataframes['df_revisao_importada'])

# DataFrame :: Compras Nacionais
df_revisao_nacional = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['df_revisao_nacional']),
                            sheet_name = arquivos_primarios['df_revisao_nacional'].split('.')[0],
                            usecols = list(tp_dado_arquivos['df_revisao_nacional'].keys()),
                            dtype = tp_dado_arquivos['df_revisao_nacional']).applymap(fx.padronizar)
df_revisao_nacional = df_revisao_nacional.rename(columns=rename_dataframes['df_revisao_nacional'])

# DataFrame :: 
df_demanda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['demanda']),
                           sheet_name = arquivos_primarios['demanda_sn'],
                           usecols = list(tp_dado_arquivos['demanda'].keys()),
                           dtype = tp_dado_arquivos['demanda'])

# DataFrame :: 
df_unidades = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_exp']),
                            sheet_name = arquivos_primarios['unidades_exp_sn'],
                            usecols = list(tp_dado_arquivos['unidades_exp'].keys()),
                            dtype = tp_dado_arquivos['unidades_exp'])

# DataFrame :: Estrutura Comercial
df_supervisoes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['mercados']),
                               sheet_name = arquivos_primarios['mercados'].split('.')[0],
                               usecols = list(tp_dado_arquivos['mercados'].keys()),
                               dtype = tp_dado_arquivos['mercados'])

# DataFrame :: 
df_terceiras = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_terceiras']),
                             sheet_name = arquivos_primarios['unidades_terceiras'].split('.')[0],
                             usecols = list(tp_dado_arquivos['unidades_terceiras'].keys()),
                             dtype = tp_dado_arquivos['unidades_terceiras'])

# DataFrame :: Dicionário Genérico
df_dicionario = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['dicgen']),
                              sheet_name = arquivos_primarios['dicgen'].split('.')[0],
                              usecols = list(tp_dado_arquivos['dicgen'].keys()),
                              dtype = tp_dado_arquivos['dicgen'])

# DataFrame :: Template Demanda
#fx.validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_demanda']))
template_demanda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_demanda']),
                            usecols = list(tp_dado_arquivos['template_demanda'].keys()),
                            dtype = tp_dado_arquivos['template_demanda'])

# DataFrame :: Template Definição Limites
#fx.validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_limites']))
template_limites = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_limites']),\
                              delimiter = ';', encoding = 'utf-8')

# DataFrame :: Template Correntes
#fx.validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_correntes']))
template_correntes = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_correntes']),\
                              delimiter = ';', encoding = 'utf-8')


# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================

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
# 2025-08-15 :: OffSet deve ser removido
# df_revisao_importada['DT_REMESSA'] = df_revisao_importada['DT_REMESSA'] - pd.offsets.MonthBegin(1)
df_revisao_importada['DT_REMESSA'] = df_revisao_importada['DT_REMESSA']
df_revisao_importada = fx.left_outer_join(df_revisao_importada,agrupamento_produtos_mp,left_on='CODIGO_MP',right_on='COD_ESPECIFICO',
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
df_revisao_nacional = fx.left_outer_join(df_revisao_nacional, agrupamento_produtos_mp, left_on = 'CODIGO_MP', right_on = 'COD_ESPECIFICO',
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

# 2025-08-18 :: Incluindo a informação de PLANTA para aumentar a granularidade do BIND.py
df_revisao = df_revisao.merge(df_correntes, how='left', left_on=['NOME_PORTO_VCM','PORTO','PLANTA'], right_on=['NOME_PORTO_VCM','PORTO','UNIDADE'])


# Tá no arquivo de supply, faz sentido ter isso em bind?
# # Salvando um dataframe com o histórico da execução para log_futuro
# exec_hist_df_revisao = df_revisao.copy()
# Linhas acima pegas em supply.py 

# MERCADOS CONSUMIDORES
# =====================
# Está seção dedica-se ao ETL para a criação dos WIZARDS de MERCADOS CONSUMIDORES
df_agrupamento = fx.left_outer_join(agrupamento_produtos_pf, pf_cadastrada, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM',
                                    name_left='Agrupamento de Produtos', name_right='Cadastro de Produtos - PF')
df_agrupamento = df_agrupamento[['CODIGO_AGRUPADO','DESCRICAO_ESPECIFICA','PRD-VCM','COD_ESPECIFICO']]
df_agrupamento = df_agrupamento.dropna(subset = ['PRD-VCM'])
df_agrupamento = df_agrupamento.drop_duplicates(subset = ['CODIGO_AGRUPADO'])
df_agrupamento = df_agrupamento.reset_index()
df_agrupamento = df_agrupamento.drop(columns = ['index'])
id_produtos_mc = df_agrupamento.copy()
id_produtos_mc = id_produtos_mc['PRD-VCM'].to_frame()

df_supervisoes['ID'] = df_supervisoes['GERENCIA'] + '-' + df_supervisoes['CONSULTORIA']
# RENOMEANDO O NOVO ARQUIVO DE DEMANDA IRRESTRITA PARA OS HEADERS ANTIGOS
rename_cols = {'CODIGO PRODUTO':'PRODUTO ID','QUANTIDADE':'VOLUME',
               'GERENCIA':'REGIONAL','CONSULTORIA':'SUPERVISAO', 'PERIODO':'PERIODO'}
df_demanda = df_demanda.rename(columns = rename_cols)
df_demanda = df_demanda.loc[df_demanda['PRODUTO ID'].notnull(),:]
df_demanda = fx.left_outer_join(df_demanda, agrupamento_produtos_pf, left_on = 'PRODUTO ID', right_on = 'COD_ESPECIFICO',
                                name_left='Demanda', name_right='Agrupamento de Produtos')
df_demanda = df_demanda.dropna(subset = ['CODIGO_AGRUPADO'])
df_demanda['Código Agrupado'] = df_demanda['CODIGO_AGRUPADO'].astype(np.int64)
df_demanda = df_demanda.drop(columns = ['PRODUTO ID','COD_ESPECIFICO'])
df_demanda = df_demanda.rename(columns = {'CODIGO_AGRUPADO':'PRODUTO ID'})
df_demanda['PRODUTO ID'] = df_demanda['PRODUTO ID'].astype('string')
# Criar uma lista de unidades terceiras para checar no arquivo de demanda
unique = df_terceiras['UNIDADE PRODUTORA'].drop_duplicates().to_list()
df_demanda['proxy.Faturamento'] = df_demanda['UNIDADE PRODUTORA'].apply(lambda x: x if x not in unique else np.nan)

# Separar o arquivo de demanda com base na existência ou não na lista "unique"
demanda_unidade_standard = df_demanda.loc[df_demanda['proxy.Faturamento'].notna(),:].reset_index().drop(columns='index')
demanda_unidade_terceira = df_demanda.loc[df_demanda['proxy.Faturamento'].isna(),:].reset_index().drop(columns='index')
# Para o primeiro caso, a unidade de faturamento é a própria unidade produtora
demanda_unidade_standard['UNIDADE FATURAMENTO'] = demanda_unidade_standard['proxy.Faturamento']
# Recriar a lista agora apenas com as consultorias relevantes para determinar a UNIDADE FATURAMENTO
unique = df_terceiras.loc[df_terceiras['CONSULTORIA'].notna(),:]['GERENCIA'].drop_duplicates().to_list()
demanda_unidade_terceira['proxy.Supervisao'] = demanda_unidade_terceira['REGIONAL'].apply(lambda x: np.nan if x not in unique else x)
demanda_unidade_terceira_na = demanda_unidade_terceira.loc[demanda_unidade_terceira['proxy.Supervisao'].isna(),:].reset_index().drop(columns='index')
demanda_unidade_terceira_notna = demanda_unidade_terceira.loc[demanda_unidade_terceira['proxy.Supervisao'].notna(),:].reset_index().drop(columns='index')
proxy = df_terceiras.loc[df_terceiras.CONSULTORIA.isna(),:].reset_index().drop(columns='index')
demanda_unidade_terceira_na = demanda_unidade_terceira_na.merge(proxy, how = 'left', left_on = ['UNIDADE PRODUTORA','REGIONAL'], right_on = ['UNIDADE PRODUTORA','GERENCIA'])
proxy = df_terceiras.loc[df_terceiras.CONSULTORIA.notna(),:].reset_index().drop(columns='index')
demanda_unidade_terceira_notna = demanda_unidade_terceira_notna.merge(proxy, how = 'left', left_on = ['UNIDADE PRODUTORA','REGIONAL','SUPERVISAO'],
                                                                      right_on = ['UNIDADE PRODUTORA','GERENCIA','CONSULTORIA'])
demanda = pd.concat([demanda_unidade_standard, demanda_unidade_terceira_na, demanda_unidade_terceira_notna])
demanda['UNIDADE PRODUTORA'] = demanda['UNIDADE PRODUTORA'].replace(list(df_dicionario['DE']), list(df_dicionario['PARA']))
demanda['UNIDADE FATURAMENTO'] = demanda['UNIDADE FATURAMENTO'].replace(list(df_dicionario['DE']), list(df_dicionario['PARA']))
df_unidades['DEPOSITO'] = np.where(df_unidades['DEPOSITO'] == '1001',
                                   df_unidades['PLANTA'],
                                   df_unidades['DEPOSITO'])
demanda['pkLEFT'] = demanda['UNIDADE PRODUTORA'] + '-' + demanda['UNIDADE FATURAMENTO']
df_unidades['pkRIGHT.2'] = df_unidades['DEPOSITO'] + '-' + df_unidades['PLANTA']
df_unidades = df_unidades.dropna(subset='pkRIGHT.2')
demanda = fx.left_outer_join(demanda, df_unidades, left_on = 'pkLEFT', right_on = 'pkRIGHT.2',
          name_left='Demanda', name_right='De-Para Unidades Expedição')
demanda = fx.left_outer_join(demanda, df_agrupamento, left_on = 'PRODUTO ID', right_on = 'CODIGO_AGRUPADO',
          name_left='Demanda', name_right='Agrupamento de Produtos')
demanda = demanda[['REGIONAL','SUPERVISAO','VOLUME','PERIODO','UNIDADE_EXPEDICAO_VCM','PRD-VCM']]
demanda = demanda.dropna(subset = ['UNIDADE_EXPEDICAO_VCM','PRD-VCM'])
demanda['Regional - Supervisão'] = demanda['REGIONAL'] + '-' + demanda['SUPERVISAO']
demanda = fx.left_outer_join(demanda, df_supervisoes, left_on = 'Regional - Supervisão', right_on = 'ID',
          name_left='Demanda', name_right='Estrutura Comercial')
demanda = demanda[['PERIODO','PRD-VCM','UNIDADE_EXPEDICAO_VCM','VCM','VOLUME']]
demanda = fx.left_outer_join(demanda, df_periodos, left_on = 'PERIODO', right_on = 'PERIODO',
          name_left='Demanda', name_right='Período')
demanda['ID Origem-Destino'] = demanda['UNIDADE_EXPEDICAO_VCM'] + '-' + demanda['VCM']
demanda = demanda.dropna(subset = ['PRD-VCM'])

#####################################################################################################
#####################################################################################################

# AMARRAÇÃO DAS CORRENTES DE CONSUMO
# ==================================
dep_correntes['ID'] = dep_correntes['Unidade-Origem'] + '-' + dep_correntes['Unidade-Destino']
demanda = fx.left_outer_join(demanda, dep_correntes, left_on = 'ID Origem-Destino', right_on = 'ID',
                             name_left='Demanda', name_right='Update Correntes')
demanda_corrente_agrupada = demanda.groupby(['ConjuntoCorrentes','NOME_PERIODO','PRD-VCM'])['VOLUME'].sum().reset_index()
demanda_corrente_agrupada = demanda_corrente_agrupada.rename(columns={'ConjuntoCorrentes':'Unidade','NOME_PERIODO':'Período','PRD-VCM':'Produto','VOLUME':'Limite'})
demanda_corrente_agrupada['Ativo'] = True

# AMARRAÇÃO DAS CORRENTES DE FORNECIMENTO
# =======================================
df_revisao_correntes_grouped = df_revisao.groupby(['CORRENTE','NOME_PERIODO','PRD-VCM'])['BALANCE_TONS'].sum().reset_index()
wizard_suprimento_amarracao = df_revisao_correntes_grouped.copy()
wizard_suprimento_amarracao = wizard_suprimento_amarracao.rename(columns={'CORRENTE':'Unidade','NOME_PERIODO':'Período', 'PRD-VCM':'Produto','BALANCE_TONS':'Limite'})
wizard_suprimento_amarracao['Ativo'] = True
wizard_suprimento_amarracao = wizard_suprimento_amarracao.loc[wizard_suprimento_amarracao['Limite']>0.0]
wizard_amarracao = pd.concat([demanda_corrente_agrupada,wizard_suprimento_amarracao])
wizard_amarracao['ID-RIGHT'] = wizard_amarracao['Unidade'] + wizard_amarracao['Período'] + wizard_amarracao['Produto']

# ATIVAÇÃO DO DETALHAMENTO POR PRODUTO
# ====================================
# Cria uma ativação por produto e por corrente
aux_wizard_amarracao = wizard_amarracao[['Unidade','Ativo']]
aux_wizard_amarracao = aux_wizard_amarracao.drop_duplicates()
template_limites = fx.left_outer_join(template_limites, aux_wizard_amarracao, left_on = 'Unidade', right_on = 'Unidade',)
                                      #name_left='Template Limites', name_right='Wizard Amarração')
template_limites.fillna(False)
for i in tqdm(range(template_limites.shape[0])):
    if template_limites['Ativo'][i] == True:
       template_limites['Nivel Detalhe'][i] = 'Detalhado por Produto'
    else:
        template_limites['Nivel Detalhe'][i] = template_limites['Nivel Detalhe'][i]

#template_limites.to_csv(os.path.join(cwd,output_path + 'tbOutDefinicaoLimites.csv'), encoding = '1252', index = False)
#print('DefinicaoLimites.xlsx deverá ser atualizada no VCM para ativar/desativar as correntes!')
print('Importante atualizar WIZARD CORRENTES INPUT a partir dos dados do VCM!')

# >> AMARRAÇÃO DAS CORRENTES DE FORNECIMENTO + CONSUMO <<
# =======================================================
template_correntes = template_correntes.astype({'Limite':str})
template_correntes['Limite'] = template_correntes['Limite'].str.replace(",",".")
template_correntes['Limite'] = template_correntes['Limite'].astype(np.float32)
template_correntes['Limite'] = 0.0
template_correntes['Ativo'] = False
template_correntes['ID-LEFT'] = template_correntes['Unidade'] + template_correntes['Periodo'] + template_correntes['Produto']
wizard_amarracao = pd.concat([demanda_corrente_agrupada,wizard_suprimento_amarracao])
wizard_amarracao['ID-RIGHT'] = wizard_amarracao['Unidade'] + wizard_amarracao['Período'] + wizard_amarracao['Produto']
template_correntes = fx.left_outer_join(template_correntes, wizard_amarracao, right_on = 'ID-RIGHT', left_on = 'ID-LEFT',
                                        name_left='Template Correntes', name_right='Wizard Amarração')
template_correntes = template_correntes.astype({'Limite_y':np.float32,'Limite_x':np.float32})
template_correntes['Limite_y'] = template_correntes['Limite_y'].fillna(0.0)
for i in tqdm(range(template_correntes.shape[0])):
    if template_correntes['Limite_y'][i] > 0.0:
        template_correntes['Limite_x'][i] = template_correntes['Limite_y'][i]
        template_correntes['Ativo_x'][i] = True
# 23/05/2024 Removendo acento de Periodo de acordo com alteração no VCM CLI
dict_cols = {'Unidade_x':'Unidade','Produto_x':'Produto','Limite_x':'Limite','Ativo_x':'Ativo'}
template_correntes = template_correntes[['Unidade_x','Periodo','Produto_x','Limite_x','Ativo_x']].rename(columns = dict_cols)
decimals_kg = 2
template_correntes['Limite'] = template_correntes['Limite'].apply(lambda x: round(x, decimals_kg))

# 12/04/2024: Alterando enconding para utf-8 conforme alinhamento com OP2B
template_correntes.to_csv(os.path.join(cwd,output_path + 'tbOutLimitesMinEntrada.csv'),\
                  index = False, encoding = 'utf-8', sep = ';')

print('Wizard de Limites :: Atualizado com Sucesso!')
end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')