print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                           >>   sku_activation.py  <<                                           ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 26/05/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 29/07/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (09/06/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
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
from tqdm import tqdm

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
print('Carregando arquivos... \n')
#print('Tempo de execução esperado: por volta de 15 segundos \n')

# DataFRame :: Dicionário Generico
dicgen = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['dicgen']),
                       usecols=list(tp_dado_arquivos['dicgen'].keys()),
                       dtype=tp_dado_arquivos['dicgen']).applymap(fx.padronizar)

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
# agrupamento_produtos = agrupamento_produtos.loc[agrupamento_produtos['TIPO_MATERIAL']=='MP']
agrupamento_produtos = agrupamento_produtos.drop(columns='TIPO_MATERIAL')
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
df_demanda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['demanda']),
                                        sheet_name = arquivos_primarios['demanda_sn'],
                                        usecols = list(tp_dado_arquivos['demanda'].keys()),
                                        dtype = tp_dado_arquivos['demanda'])
df_demanda['UNIDADE PRODUTORA'] = df_demanda['UNIDADE PRODUTORA'].replace(list(dicgen['DE']), list(dicgen['PARA']))

# DataFrame :: Unidades de Expedição e Descarga
df_expedicao = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_exp']),
                         sheet_name= arquivos_primarios['unidades_exp'].split('.')[0],
                       usecols=list(tp_dado_arquivos['unidades_exp'].keys()),
                       dtype=tp_dado_arquivos['unidades_exp']).applymap(fx.padronizar)
#df_expedicao = df_expedicao['DEPOSITO','PLANTA','DESCRICAO_DEPOSITO','DESCRICAO_PLANTA','TIPO_UNIDADE','UP_MISTURADORA_VCM','UP_EMBALADORA_VCM']

# DataFrame :: Estrutura Comercial
df_comercial = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['mercados']),
                         sheet_name= arquivos_primarios['mercados'].split('.')[0],
                       usecols=list(tp_dado_arquivos['mercados'].keys()),
                       dtype=tp_dado_arquivos['mercados']).applymap(fx.padronizar)

# DataFrame :: Unidades Terceiras
df_gerencia = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_terceiras']),
                       sheet_name= arquivos_primarios['unidades_terceiras'].split('.')[0],
                       usecols=list(tp_dado_arquivos['unidades_terceiras'].keys()),
                       dtype=tp_dado_arquivos['unidades_terceiras']).applymap(fx.padronizar)
df_gerencia['UNIDADE PRODUTORA'] = df_gerencia['UNIDADE PRODUTORA'].replace(list(dicgen['DE']), list(dicgen['PARA']))
df_gerencia['UNIDADE FATURAMENTO'] = df_gerencia['UNIDADE FATURAMENTO'].replace(list(dicgen['DE']), list(dicgen['PARA']))

# DataFrame :: Update Correntes
up_correntes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['arq_tbUpdateCorrentes']),
                         sheet_name= arquivos_primarios['arq_tbUpdateCorrentes'].split('.')[0],
                       usecols=list(tp_dado_arquivos['arq_tbUpdateCorrentes'].keys()),
                       dtype=tp_dado_arquivos['arq_tbUpdateCorrentes']).applymap(fx.padronizar)

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
df_revisao_importada = fx.left_outer_join(df_revisao_importada, agrupamento_produtos, left_on='CODIGO_MP', right_on='COD_ESPECIFICO',
                                          name_left = 'Revisão de Chegadas >>Importada<<', name_right = 'Agrupamento de Produtos')
df_revisao_importada = fx.left_outer_join(df_revisao_importada, df_periodos, left_on = 'DT_REMESSA', right_on = 'PERIODO',
                                          name_left = 'Revisão de Chegadas >>Importada<<', name_right = 'Períodos')
df_revisao_importada = fx.left_outer_join(df_revisao_importada, cadastro_mp, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM',
                                          name_left = 'Revisão de Chegadas >>Importada<<', name_right = 'Cadastro de Produtos VCM')

# 2.2. Nacional
id_vars = ['PORTO','PLANTA','MP','STATUS','COMPANY','CODIGO_MP']
df_revisao_nacional = df_revisao_nacional.melt(id_vars = id_vars, var_name = 'PROXY_PERIODO',
                                               value_name = 'BALANCE_TONS')
df_revisao_nacional['COMPANY'] = df_revisao_nacional['COMPANY'].replace(companies)
df_revisao_nacional['PORTO'] = df_revisao_nacional['PORTO'] + '-' + df_revisao_nacional['PLANTA']
df_revisao_nacional = fx.left_outer_join(df_revisao_nacional, agrupamento_produtos, left_on = 'CODIGO_MP', right_on = 'COD_ESPECIFICO',
                                         name_left='Revisão de Chegadas >>Nacional<<', name_right='Agrupamento de Produtos')
df_revisao_nacional = fx.left_outer_join(df_revisao_nacional, df_periodos, left_on = 'PROXY_PERIODO', right_on = 'pk_NOME_PERIODO',
                                         name_left='Revisão de Chegadas >>Nacional<<', name_right='Períodos')
df_revisao_nacional = fx.left_outer_join(df_revisao_nacional, cadastro_mp, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM',
                                         name_left='Revisão de Chegadas >>Nacional<<', name_right='Cadastro de Produtos VCM')

# 3. DataFrame de Compras Completo :: Importado + Nacional
cols = ['PORTO','PLANTA','MP','COMPANY','CODIGO_MP','COD_ESPECIFICO','CODIGO_AGRUPADO','PERIODO','NOME_PERIODO','PRD-VCM','DESCRICAO','TIPO_MATERIAL','CATEGORIA','BALANCE_TONS']
df_revisao_importada = df_revisao_importada[cols]
df_revisao_nacional = df_revisao_nacional[cols]
df_revisao = pd.concat([df_revisao_importada,df_revisao_nacional])
df_revisao = df_revisao.reset_index().drop(columns='index')
df_revisao = fx.left_outer_join(df_revisao, df_portos, left_on = 'PORTO', right_on = 'PORTO',
                                name_left='Revisão de Chegadas >>ALL<<', name_right='Portos')

# Parte acima pega de supply, resto é de warehouses!
# ==============================================================================
df_revisao['PORTO']=df_revisao['PORTO'].astype('string')

df_revisao['Origem-Destino'] = ''
for j in range(df_revisao.shape[0]):
    df_revisao['Origem-Destino'][j] = df_revisao['PORTO'][j] + '-' + df_revisao['PLANTA'][j]
id_produtos = mp_fornecimento_nacional['PRD-VCM-NAC'].to_frame().rename(columns={'PRD-VCM-NAC':'PRD-VCM'})
portos_correntes['ID_correntes'] = ''
for z in range(portos_correntes.shape[0]):
    portos_correntes['ID_correntes'][z] = portos_correntes['PORTO'][z] + '-' + portos_correntes['UNIDADE'][z]

id_correntes = portos_correntes['CORRENTE']
id_correntes = id_correntes.drop_duplicates().to_frame()

df_comercial['ID'] = df_comercial['GERENCIA'] + '-' + df_comercial['CONSULTORIA']
# RENOMEANDO O NOVO ARQUIVO DE DEMANDA IRRESTRITA PARA OS HEADERS ANTIGOS
rename_cols = {'CODIGO PRODUTO':'PRODUTO ID','QUANTIDADE':'VOLUME',
               'GERENCIA':'REGIONAL','CONSULTORIA':'SUPERVISAO', 'PERIODO':'PERIODO'}
df_demanda = df_demanda.rename(columns = rename_cols)
df_demanda = fx.left_outer_join(df_demanda,agrupamento_produtos,left_on='PRODUTO ID',right_on='COD_ESPECIFICO',
                                name_left='Demanda', name_right='Agrupamento de Produtos')
df_demanda = fx.left_outer_join(df_demanda, cadastro_pf, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM',
                                name_left='Demanda', name_right='Cadastro de Produtos - Filtro PF')
df_demanda = df_demanda.drop_duplicates(subset=['PERIODO','DIRETORIA','REGIONAL','PRODUTO ID'])
df_demanda = df_demanda.dropna(subset = ['CODIGO_AGRUPADO'])
df_demanda = df_demanda.drop(columns = ['PRODUTO ID','COD_ESPECIFICO'])
df_demanda = df_demanda.rename(columns = {'CODIGO_AGRUPADO':'PRODUTO ID'})
unique = df_gerencia['UNIDADE PRODUTORA'].drop_duplicates().to_list()
df_demanda['proxy.Faturamento'] = df_demanda['UNIDADE PRODUTORA'].apply(lambda x: x if x not in unique else np.nan)

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
demanda_unidade_terceira['proxy.Supervisao'] = demanda_unidade_terceira['REGIONAL'].apply(lambda x: np.nan if x not in unique else x)
demanda_unidade_terceira_na = demanda_unidade_terceira.loc[demanda_unidade_terceira['proxy.Supervisao'].isna(),:].reset_index().drop(columns='index')
demanda_unidade_terceira_notna = demanda_unidade_terceira.loc[demanda_unidade_terceira['proxy.Supervisao'].notna(),:].reset_index().drop(columns='index')
proxy = df_gerencia.loc[df_gerencia.CONSULTORIA.isna(),:].reset_index().drop(columns='index')
demanda_unidade_terceira_na = demanda_unidade_terceira_na.merge(proxy,how='left',left_on=['UNIDADE PRODUTORA','REGIONAL'],right_on=['UNIDADE PRODUTORA','GERENCIA'])
# Desativando aqui pq não precisa manter o msm tmanho (eu acho)
#fx.left_outer_join(demanda_unidade_terceira_na,proxy,left_on=['UNIDADE PRODUTORA','REGIONAL'],right_on=['UNIDADE PRODUTORA','GERENCIA'], struct=False)
proxy = df_gerencia.loc[df_gerencia.CONSULTORIA.notna(),:].reset_index().drop(columns='index')
proxy['CONSULTORIA'] = proxy['CONSULTORIA'].astype(str)
demanda_unidade_terceira_notna = demanda_unidade_terceira_notna.merge(proxy,how='left',left_on=['UNIDADE PRODUTORA','REGIONAL','SUPERVISAO'],right_on=['UNIDADE PRODUTORA','GERENCIA','CONSULTORIA'])
# Desativando aqui pq não precisa manter o msm tmanho (eu acho)
#fx.left_outer_join(demanda_unidade_terceira_notna,proxy,left_on=['UNIDADE PRODUTORA','REGIONAL','SUPERVISAO'],right_on=['UNIDADE PRODUTORA','GERENCIA','CONSULTORIA'], struct=False)
df_demanda = pd.concat([demanda_unidade_standard, demanda_unidade_terceira_na, demanda_unidade_terceira_notna])

df_expedicao['DEPOSITO'] = np.where(df_expedicao['DEPOSITO'] == '1001',
                                   df_expedicao['PLANTA'],
                                   df_expedicao['DEPOSITO'])
df_demanda['UNIDADE-LEFT'] = df_demanda['UNIDADE PRODUTORA'] + '-' + df_demanda['UNIDADE FATURAMENTO']
df_expedicao['UNIDADE-RIGHT'] = df_expedicao['DEPOSITO'] + '-' + df_expedicao['PLANTA']
df_expedicao = df_expedicao.dropna(subset=["UNIDADE-RIGHT"])
#df_demanda = df_demanda.merge(df_expedicao, left_on = 'UNIDADE-LEFT', right_on = 'UNIDADE-RIGHT', how = 'left')
df_demanda = fx.left_outer_join(df_demanda, df_expedicao, left_on = 'UNIDADE-LEFT', right_on = 'UNIDADE-RIGHT',
                                name_left='Demanda', name_right='Unidades de Expedição')
df_demanda = df_demanda[['UNIDADE PRODUTORA','UNIDADE FATURAMENTO','REGIONAL','SUPERVISAO','VOLUME','PERIODO','UNIDADE_ARMAZENAGEM_VCM','UNIDADE_EXPEDICAO_VCM','PRD-VCM']] # VER SE É UNIDADE ARMAZENAGEM OU EXPEDIÇÃO
df_demanda['Regional - Supervisão'] = df_demanda['REGIONAL'] + '-' + df_demanda['SUPERVISAO']
df_demanda = fx.left_outer_join(df_demanda, df_comercial, left_on='Regional - Supervisão', right_on='ID',
                                name_left='Demanda', name_right='Estrutura Comercial')
df_demanda = df_demanda[['UNIDADE PRODUTORA','UNIDADE FATURAMENTO','PERIODO','PRD-VCM','UNIDADE_ARMAZENAGEM_VCM','UNIDADE_EXPEDICAO_VCM','VOLUME','VCM']]
df_demanda = fx.left_outer_join(df_demanda, df_periodos, left_on = 'PERIODO', right_on = 'PERIODO',
                                name_left='Demanda', name_right='Períodos')

# UNICO QUE ACHOU ALGUMA CORRESPONDÊNCIA FOI expediçãoVCM
df_demanda['ID Origem-Destino'] = df_demanda['UNIDADE_EXPEDICAO_VCM'] + '-' + df_demanda['VCM']
df_demanda = df_demanda.dropna(subset = ['PRD-VCM'])
up_correntes['ID'] = up_correntes['Unidade-Origem'] + '-' + up_correntes['Unidade-Destino']
df_demanda = fx.left_outer_join(df_demanda, up_correntes, left_on='ID Origem-Destino', right_on='ID',
                                name_left='Demanda', name_right='Update Correntes')
demanda_corrente_agrupada = df_demanda.groupby(['ConjuntoCorrentes','PERIODO','PRD-VCM'])['VOLUME'].sum().reset_index()
demanda_corrente_agrupada = demanda_corrente_agrupada.rename(columns={'ConjuntoCorrentes':'Unidade','PERIODO':'Período','PRD-VCM':'Produto','VOLUME':'Limite'})
demanda_corrente_agrupada['Ativo'] = True

# ##########################################################
# ##########################################################

# # AMARRAÇÃO DAS CORRENTES DE CONSUMO
# # ==================================
df_revisao = fx.left_outer_join(df_revisao, portos_correntes, left_on='Origem-Destino',right_on='ID_correntes',
                                name_left='Revisão de Chegadas >>ALL<<', name_right='Granularidade de Correntes')
df_revisao_correntes_grouped = df_revisao.groupby(['CORRENTE','PERIODO','PRD-VCM'])['BALANCE_TONS'].sum().reset_index()
wizard_suprimento_amarracao = df_revisao_correntes_grouped.copy()
wizard_suprimento_amarracao = wizard_suprimento_amarracao.rename(columns={'CORRENTE':'Unidade','PERIODO':'Período', 'PRD-VCM':'Produto','BALANCE_TONS':'Limite'})
wizard_suprimento_amarracao['Ativo'] = True
# Faz sentido eu adicionar isso? Só é para estar True, onde for >0.0 né?
wizard_suprimento_amarracao = wizard_suprimento_amarracao.loc[wizard_suprimento_amarracao['Limite']>0.0]
wizard_amarracao = pd.concat([demanda_corrente_agrupada,wizard_suprimento_amarracao])
wizard_amarracao['Período'] =  wizard_amarracao['Período'].astype(str)
wizard_amarracao['ID-RIGHT'] = wizard_amarracao['Unidade'] + wizard_amarracao['Período'] + wizard_amarracao['Produto']

# ATIVAÇÃO DO DETALHAMENTO POR PRODUTO
# ====================================
aux_wizard_amarracao = wizard_amarracao[['Unidade','Ativo']]
aux_wizard_amarracao = aux_wizard_amarracao.drop_duplicates()
template_limites = fx.left_outer_join(template_limites, aux_wizard_amarracao, left_on = 'Unidade', right_on = 'Unidade',
                                      name_left='Template', name_right='Wizard Amarração')
template_limites['Ativo'] = template_limites['Ativo'].fillna(False)
for i in tqdm(range(template_limites.shape[0])):
    if template_limites['Ativo'][i] == True:
       template_limites['Nivel Detalhe'][i] = 'Detalhado por Produto'
    else:
        template_limites['Nivel Detalhe'][i] = 'Sem Definição'

# (29/07/2025) Retirando a coluna "Ativo" como foi pedido pelo time de OP2B
template_limites = template_limites.drop(columns=['Ativo'])
template_limites.to_csv(os.path.join(cwd,output_path + 'tbOutDefinicaoLimMinEnt.csv'), sep = ';', encoding = 'utf-8-sig', index = False)
print('Arquivo (tbOutDefinicaoLimMinEnt.xlsx) foi Atualizado com Sucesso!')
print('tbOutDefinicaoLimMinEnt.xlsx deverá ser atualizada no VCM para ativar/desativar as correntes!')
print('Importante atualizar WIZARD CORRENTES INPUT a partir dos dados do VCM!')


# >> AMARRAÇÃO DAS CORRENTES DE FORNECIMENTO + CONSUMO <<
# =================================================

#correntes = 'tbTemplateCorrentes.csv'
#correntes = pd.read_csv(os.path.join(cwd,path + correntes), delimiter=';', encoding='utf-8')
# 19/07/2023: Changing the loading type of the csv file to UTF-8 and with delimiter = ,
# 18/12/2023: Alterado formato do csv para encoding = '1252' e delimiter = ';'
#correntes = pd.read_csv(os.path.join(cwd,structure_path + correntes), delimiter=';', encoding='1252')

#correntes = correntes.astype({'Limite':str})
#correntes['Limite'] = correntes['Limite'].str.replace(",",".")
#correntes['Limite'] = correntes['Limite'].astype(np.float32)
#correntes['Limite'] = 0.0
#correntes['Ativo'] = False
#correntes['ID-LEFT'] = correntes['Unidade'] + correntes['Periodo'] + correntes['Produto']
#wizard_amarracao = demanda_corrente_agrupada.append(wizard_suprimento_amarracao, ignore_index=True)
#wizard_amarracao = pd.concat([demanda_corrente_agrupada,wizard_suprimento_amarracao])
#wizard_amarracao['ID-RIGHT'] = wizard_amarracao['Unidade'] + wizard_amarracao['Período'] + wizard_amarracao['Produto']
#correntes = correntes.merge(wizard_amarracao, how = 'left', right_on = 'ID-RIGHT', left_on = 'ID-LEFT')
#correntes = correntes.astype({'Limite_y':np.float32,'Limite_x':np.float32})
#correntes['Limite_y'] = correntes['Limite_y'].fillna(0.0)
#for i in tqdm(range(correntes.shape[0])):
#    if correntes['Limite_y'][i] > 0.0:
#        correntes['Limite_x'][i] = correntes['Limite_y'][i]
#        correntes['Ativo_x'][i] = True
#dict_cols = {'Unidade_x':'Unidade','Produto_x':'Produto','Limite_x':'Limite','Ativo_x':'Ativo'}
#correntes = correntes[['Unidade_x','Período','Produto_x','Limite_x','Ativo_x']].rename(columns = dict_cols)
#decimals_kg = 2
#correntes['Limite'] = correntes['Limite'].apply(lambda x: round(x, decimals_kg))

# Deprecado a exportação para excel e alterado para csv
# encoding = '1252' e delimiter = ';'
#correntes.to_excel(os.path.join(cwd,output_path + 'Wizard_Amarracao.xlsx'), index = False)
#correntes.to_csv(os.path.join(cwd,output_path + 'Wizard de Limites.csv'), index = False, encoding = '1252', sep = ';')
#print('Wizard de Limites :: Atualizado com Sucesso!')

end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')