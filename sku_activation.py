print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                           >>   sku_activation.py  <<                                           ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 26/05/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 26/05/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (26/05/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Definição de Limites                                                                                        ║')
print('║ >> Ativa ou desativa o detalhamento de uma unidade ou corrente em função de produto.                           ║')
print('║                                                                                                                ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
print('\n')

# =======================================================================================================================
# IMPORTAR BIBLIOTECAS
# =======================================================================================================================

import os
import sys
import time
import datetime
import pandas as pd
import numpy as np
import warnings
from tkinter import messagebox
warnings.filterwarnings('ignore')
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import MonthBegin
from unidecode import unidecode

# =======================================================================================================================
# CONFIGURAÇÕES INICIAIS
# =======================================================================================================================

start_time = time.time()
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

# DataFrame :: Horizonte (Período) de Otimização
df_periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']), 
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])

# applymap(padronizar) não aplicado por se tratar de dados com a estrutura final do VCM
df_periodos['pk_NOME_PERIODO'] = df_periodos['NOME_PERIODO'].str.split(' ', expand = True)[0]
id_periodos = df_periodos['NOME_PERIODO'].to_frame()

# DataFrame :: Chaves identificadores dos Portos
# DataFrame :: portos :: dado primário
df_portos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['portos']), 
                      sheet_name = arquivos_primarios['portos'].split('.')[0],
                      usecols = list(tp_dado_arquivos['portos'].keys()),
                      dtype = tp_dado_arquivos['portos']).applymap(fx.padronizar)

# DataFrame :: portos_correntes :: granularidade de correntes
portos_correntes = df_portos.copy()

# ================================
# TALVEZ NÃO SEJA UNITILIZADO NESSE SCRIPT :) (vamo ver isso ae)
# DataFrame :: az_portos :: unidades de armazenagem dos portos
az_portos = df_portos.copy()
az_portos = df_portos[['NOME_AZ_PORTO_VCM','PORTO']].drop_duplicates()

# DataFrame :: postos :: apenas a nível de PORTO e NOME_PORTO_VCM
df_portos = df_portos[['NOME_PORTO_VCM','PORTO']].drop_duplicates()

# DataFrame :: compras importadas :: importa todas as compras firmes IMPORTADAS ou NACIONALIZADAS
df_revisao_importada = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['df_revisao_importada']),
                           sheet_name = arquivos_primarios['df_revisao_importada'].split('.')[0],
                           usecols = list(tp_dado_arquivos['df_revisao_importada'].keys()),
                           dtype = tp_dado_arquivos['df_revisao_importada']).applymap(fx.padronizar)
df_revisao_importada = df_revisao_importada.rename(columns=rename_dataframes['df_revisao_importada'])

# DataFrame :: compras importadas :: importa todas as compras firmes NACIONAIS
df_revisao_nacional = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['df_revisao_nacional']),
                                    sheet_name = arquivos_primarios['df_revisao_nacional'].split('.')[0],
                                    usecols = list(tp_dado_arquivos['df_revisao_nacional'].keys()),
                                    dtype = tp_dado_arquivos['df_revisao_nacional']).applymap(fx.padronizar)
df_revisao_nacional = df_revisao_nacional.rename(columns=rename_dataframes['df_revisao_nacional'])

# DataFrame :: cadastro de materiais :: busca toda a lista de materiais (MP, PI, PF) no cadastrados VCM
cadastro_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                            sheet_name = arquivos_primarios['cadastro_produtos_sn01'],
                            usecols = list(tp_dado_arquivos['cadastro_produtos_sn01'].keys()),
                            dtype = tp_dado_arquivos['cadastro_produtos_sn01']).applymap(fx.padronizar)

# DataFrame :: cadastro de matérias-primas :: filtro no tipo de material da tabela CADASTRO
cadastro_mp = cadastro_produtos[(cadastro_produtos['TIPO_MATERIAL'].str.split('-',expand=True)[0].str.strip() == 'MP')]

# DataFrame :: cadastro de produto final :: filtro no tipo de material da tabela CADASTRO
cadastro_pf = cadastro_produtos[(cadastro_produtos['TIPO_MATERIAL'].str.split('-',expand=True)[0].str.strip() == 'PF')]

# DataFrame :: agrupamento de materiais :: busca todo o de-para de códigos específicos em códigos agrupados
agrupamento_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                            sheet_name = arquivos_primarios['cadastro_produtos_sn02'],
                            usecols = list(tp_dado_arquivos['cadastro_produtos_sn02'].keys()),
                            dtype = tp_dado_arquivos['cadastro_produtos_sn02']).applymap(fx.padronizar)

proxy_agrupamento = cadastro_produtos[['CODIGO_ITEM','DESCRICAO']]
proxy_agrupamento = proxy_agrupamento.rename(columns={'CODIGO_ITEM':'COD_ESPECIFICO','DESCRICAO':'DESCRICAO_ESPECIFICA'})
proxy_agrupamento['CODIGO_AGRUPADO'] = proxy_agrupamento['COD_ESPECIFICO']
proxy_agrupamento['AGRUPAMENTO_MP'] = proxy_agrupamento['DESCRICAO_ESPECIFICA']
agrupamento_produtos = pd.concat([agrupamento_produtos,proxy_agrupamento])
agrupamento_produtos = agrupamento_produtos.drop_duplicates(subset = 'COD_ESPECIFICO')

# DataFrame :: dado primário de capacidade portuária
capacidade_portos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['capacidade_portos']),
                                  sheet_name = arquivos_primarios['capacidade_portos'].split('.')[0],
                                  # uselcols = omitido por conta da consulta dinâmica de horizonte de portos
                                  dtype = tp_dado_arquivos['capacidade_portos']).applymap(fx.padronizar)

# DataFrame :: Demanda Irrestrita
df_demanda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['arq_demanda_irrestrita']),
                                        sheet_name = arquivos_primarios['arq_demanda_irrestrita_sn01'],
                                        usecols = list(tp_dado_arquivos['arq_demanda_irrestrita'].keys()),
                                        dtype = tp_dado_arquivos['arq_demanda_irrestrita'])

# DataFrame :: Unidades de Expedição e Descarga
df_expedicao = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_exp']),
                         sheet_name= arquivos_primarios['unidades_exp'].split('.')[0], 
                       usecols=list(tp_dado_arquivos['unidades_exp'].keys()),
                       dtype=tp_dado_arquivos['unidades_exp']).applymap(fx.padronizar)
#df_expedicao = df_expedicao['DEPOSITO','PLANTA','DESCRICAO_DEPOSITO','DESCRICAO_PLANTA','TIPO_UNIDADE','UP_MISTURADORA_VCM','UP_EMBALADORA_VCM']

# DataFrame :: Estrutura Comercial
df_comercial = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['arq_tbDeparaMercadoConsumidor']),
                         sheet_name= arquivos_primarios['arq_tbDeparaMercadoConsumidor'].split('.')[0], 
                       usecols=list(tp_dado_arquivos['arq_tbDeparaMercadoConsumidor'].keys()),
                       dtype=tp_dado_arquivos['arq_tbDeparaMercadoConsumidor']).applymap(fx.padronizar)

# DataFRame :: Dicionário Generico
dicgen = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['dicgen']), 
                       usecols=list(tp_dado_arquivos['dicgen'].keys()),
                       dtype=tp_dado_arquivos['dicgen']).applymap(fx.padronizar)

# DataFrame :: Unidades Terceiras
df_gerencia = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_terceiras']),
                        # sheet_name= arquivos_primarios['unidades_terceiras'].split('.')[0], 
                       usecols=list(tp_dado_arquivos['unidades_terceiras'].keys()),
                       dtype=tp_dado_arquivos['unidades_terceiras']).applymap(fx.padronizar)
df_gerencia['UNIDADE PRODUTORA'] = df_gerencia['UNIDADE PRODUTORA'].replace(list(dicgen['DE']), list(dicgen['PARA']))
df_gerencia['UNIDADE FATURAMENTO'] = df_gerencia['UNIDADE FATURAMENTO'].replace(list(dicgen['DE']), list(dicgen['PARA']))

# DataFrame :: Update Correntes
up_correntes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['arq_tbUpdateCorrentes']),
                         sheet_name= arquivos_primarios['arq_tbUpdateCorrentes'].split('.')[0], 
                       usecols=list(tp_dado_arquivos['arq_tbUpdateCorrentes'].keys()),
                       dtype=tp_dado_arquivos['arq_tbUpdateCorrentes']).applymap(fx.padronizar)

# DataFrame :: template da Demanda
# Desativando essa validação por solicitação dos usuários (2024-10-18)
#fx.validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_demanda']))
template_demanda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_demanda']),
                                        sheet_name = arquivos_primarios['template_demanda_sn'],
                                        usecols = list(tp_dado_arquivos['template_demanda'].keys()),
                                        dtype = tp_dado_arquivos['template_demanda'])

# DataFrame :: template de Limites
#fx.validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_limites']))
template_limites = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_limites']),
                       delimiter = ';', encoding = 'utf-8-sig',
                       usecols=list(tp_dado_arquivos['template_limites'].keys()),
                       dtype=tp_dado_arquivos['template_limites'])


# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================

# 1. Definindo matérias-primas com fornecimento nacional
# ============================== Talvez add em mp_for_nac o período =================================
mp_fornecimento_nacional = cadastro_mp[(cadastro_mp['TIPO_MATERIAL'] == 'MP - COMPRAS')]['PRD-VCM']
mp_fornecimento_nacional = mp_fornecimento_nacional.reset_index().drop(columns='index')
mp_fornecimento_nacional = mp_fornecimento_nacional.rename(columns={'PRD-VCM':'PRD-VCM-NAC'})
mp_fornecimento_nacional = mp_fornecimento_nacional.drop_duplicates()

# 2. Tratando arquivos de plano de compras IMPORTADO e NACIONAL
# 2.1. Importado
companies = {'FTO':'E600','FH':'E900','SAL':'E890','CMISS':'E890','FHG':'E900','ECFTO':'E600','SFT':'E890'}
df_revisao_importada['COMPANY'] = df_revisao_importada['COMPANY'].replace(companies)
df_revisao_importada = df_revisao_importada[(df_revisao_importada['STATUS'] == 'COMPRADO')]
df_revisao_importada['DT_REMESSA'] = df_revisao_importada['DT_REMESSA'] - pd.offsets.MonthBegin(1)
fx.left_outer_join(df_revisao_importada, agrupamento_produtos, left_on='CODIGO_MP', right_on='COD_ESPECIFICO')
fx.left_outer_join(df_revisao_importada, df_periodos, left_on = 'DT_REMESSA', right_on = 'PERIODO')
fx.left_outer_join(df_revisao_importada, cadastro_mp, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM')

# 2.2. Nacional
id_vars = ['PORTO','PLANTA','MP','STATUS','COMPANY','CODIGO_MP']
df_revisao_nacional = df_revisao_nacional.melt(id_vars = id_vars, var_name = 'PROXY_PERIODO',
                                               value_name = 'BALANCE_TONS')
df_revisao_nacional['COMPANY'] = df_revisao_nacional['COMPANY'].replace(companies)
df_revisao_nacional['PORTO'] = df_revisao_nacional['PORTO'] + '-' + df_revisao_nacional['PLANTA']
fx.left_outer_join(df_revisao_nacional, agrupamento_produtos, left_on = 'CODIGO_MP', right_on = 'COD_ESPECIFICO')
fx.left_outer_join(df_revisao_nacional, df_periodos, left_on = 'PROXY_PERIODO', right_on = 'pk_NOME_PERIODO')
fx.left_outer_join(df_revisao_nacional, cadastro_mp, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM')

# 3. DataFrame de Compras Completo :: Importado + Nacional
cols = ['PORTO','PLANTA','MP','COMPANY','CODIGO_MP','COD_ESPECIFICO','CODIGO_AGRUPADO','PERIODO','NOME_PERIODO','PRD-VCM','DESCRICAO','TIPO_MATERIAL','CATEGORIA','BALANCE_TONS']
df_revisao_importada = df_revisao_importada[cols]
df_revisao_nacional = df_revisao_nacional[cols]
df_revisao = pd.concat([df_revisao_importada,df_revisao_nacional])
df_revisao = df_revisao.reset_index().drop(columns='index')
fx.left_outer_join(df_revisao, df_portos, left_on = 'PORTO', right_on = 'PORTO')

# Parte acima pega de supply, resto é de warehouses!
# ==============================================================================
df_revisao['PORTO']=df_revisao['PORTO'].astype('string')

# VERIFICAR NECESSIDADE DESSA ETAPA
# ===========================================================================================================
df_revisao['Origem-Destino'] = ''
for j in range(df_revisao.shape[0]):
    df_revisao['Origem-Destino'][j] = df_revisao['PORTO'][j] + '-' + df_revisao['PLANTA'][j]
id_produtos = mp_fornecimento_nacional['PRD-VCM-NAC'].to_frame().rename(columns={'PRD-VCM-NAC':'PRD-VCM'})
portos_correntes['ID_correntes'] = ''
for z in range(portos_correntes.shape[0]):
    portos_correntes['ID_correntes'][z] = portos_correntes['PORTO'][z] + '-' + portos_correntes['UNIDADE'][z]

id_correntes = portos_correntes['CORRENTE']
id_correntes = id_correntes.drop_duplicates().to_frame()
# ===========================================================================================================

df_comercial['ID'] = df_comercial['GERENCIA'] + '-' + df_comercial['CONSULTORIA']
# RENOMEANDO O NOVO ARQUIVO DE DEMANDA IRRESTRITA PARA OS HEADERS ANTIGOS
rename_cols = {'CODIGO PRODUTO':'PRODUTO ID','QUANTIDADE':'VOLUME',
               'GERENCIA':'REGIONAL','CONSULTORIA':'SUPERVISAO', 'PERIODO':'PERIODO'}
df_demanda = df_demanda.rename(columns = rename_cols)
fx.left_outer_join(df_demanda,agrupamento_produtos,left_on='PRODUTO ID',right_on='COD_ESPECIFICO')
fx.left_outer_join(df_demanda, cadastro_pf, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM')
df_demanda = df_demanda.drop_duplicates(subset=['PERIODO','DIRETORIA','REGIONAL','PRODUTO ID'])
df_demanda = df_demanda.dropna(subset = ['CODIGO_AGRUPADO'])
df_demanda = df_demanda.drop(columns = ['PRODUTO ID','COD_ESPECIFICO'])
df_demanda = df_demanda.rename(columns = {'CODIGO_AGRUPADO':'PRODUTO ID'})
unique = df_gerencia['UNIDADE PRODUTORA'].drop_duplicates().to_list()
df_demanda['proxy.Faturamento'] = df_demanda['UNIDADE PRODUTORA'].apply(lambda x: x if x not in unique else np.NaN)

# Separar o arquivo de demanda com base na existência ou não na lista "unique"
print('Identificando unidades de faturamento próprias da Eurochem...')
demanda_unidade_standard = df_demanda.loc[df_demanda['proxy.Faturamento'].notna(),:].reset_index().drop(columns='index')
print('Identificando unidades de faturamento para as unidades terceiras da Eurochem...')
demanda_unidade_terceira = df_demanda.loc[df_demanda['proxy.Faturamento'].isna(),:].reset_index().drop(columns='index')
print(f"Foram identificados {df_demanda.loc[df_demanda['proxy.Faturamento'].isna(),:].shape[0]} linhas da demanda com problemas na Estrutura Comercial")
print('Verificar arquivo: LOG ERROR - Combinações Gerência x Consultoria x Unidade Produtora')
log_erro_gerencias = df_demanda.loc[df_demanda['proxy.Faturamento'].isna(),:][['REGIONAL','SUPERVISAO','UNIDADE PRODUTORA']].drop_duplicates()
log_erro_gerencias.to_excel(os.path.join(cwd,exec_log_path+'LOG ERROR - Combinações Gerência x Consultoria x Unidade Produtora.xlsx'))

# Para o primeiro caso, a unidade de faturamento é a própria unidade produtora
demanda_unidade_standard['UNIDADE FATURAMENTO'] = demanda_unidade_standard['proxy.Faturamento']

# Recriar a lista agora apenas com as consultorias relevantes para determinar a UNIDADE FATURAMENTO
unique = df_gerencia.loc[df_gerencia['CONSULTORIA'].notna(),:]['GERENCIA'].drop_duplicates().to_list()
demanda_unidade_terceira['proxy.Supervisao'] = demanda_unidade_terceira['REGIONAL'].apply(lambda x: np.NaN if x not in unique else x)
demanda_unidade_terceira_na = demanda_unidade_terceira.loc[demanda_unidade_terceira['proxy.Supervisao'].isna(),:].reset_index().drop(columns='index')
demanda_unidade_terceira_notna = demanda_unidade_terceira.loc[demanda_unidade_terceira['proxy.Supervisao'].notna(),:].reset_index().drop(columns='index')
proxy = df_gerencia.loc[df_gerencia.CONSULTORIA.isna(),:].reset_index().drop(columns='index')
x0 = demanda_unidade_terceira_na.shape[0]
fx.left_outer_join(demanda_unidade_terceira_na,proxy,left_on=['UNIDADE PRODUTORA','REGIONAL'],right_on=['Unidade Produtora','Gerencia'])
proxy = df_gerencia.loc[df_gerencia.CONSULTORIA.notna(),:].reset_index().drop(columns='index')
fx.left_outer_join(demanda_unidade_terceira_notna,proxy,left_on=['UNIDADE PRODUTORA','REGIONAL','SUPERVISAO'],right_on=['Unidade Produtora','Gerencia','Consultoria'])
df_demanda = pd.concat([demanda_unidade_standard, demanda_unidade_terceira_na, demanda_unidade_terceira_notna])
df_demanda['UNIDADE-LEFT'] = df_demanda['UNIDADE PRODUTORA'] + '-' + df_demanda['UNIDADE FATURAMENTO']

df_demanda['UNIDADE-LEFT'] = df_demanda['UNIDADE PRODUTORA'] + '-' + df_demanda['UNIDADE FATURAMENTO']

df_expedicao['UNIDADE-RIGHT'] = df_expedicao['DEPOSITO'] + '-' + df_expedicao['PLANTA']
df_demanda = df_demanda.merge(df_expedicao, left_on = 'UNIDADE-LEFT', right_on = 'UNIDADE-RIGHT', how = 'left')
