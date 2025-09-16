print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                         ATUALIZACAO DE DADOS - VCM                                             ║')
print('║                                               >> supply.py <<                                                  ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado  por: Murilo Lima Ribeiro  Data: 10/03/2025                                                             ║')
print('║ Editado por: Murilo Lima Ribeiro  Data: 16/09/2025                                                             ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v2.0.0 (25/08/2025): Release Projeto Merger                                                                  ║')
print('║ - v2.1.0 (16/09/2025): Remoção de restrição do M+3                                                             ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Plano de Compras                                                                                            ║')
print('║ >> Capacidades Portuárias                                                                                      ║')
print('║ >> Leadtimes                                                                                                   ║')
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
import inspect
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

# Caminho geral dos arquivos
cwd = os.getcwd()

# Caminhos dos arquivos
structure_path = 'Structural Data/'         
path = 'Input Data/'                     
output_path = 'Output Data/'               
exec_log_path = 'Error Logs/'

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
periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']), 
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])

# applymap(padronizar) não aplicado por se tratar de dados com a estrutura final do VCM
periodos['pk_NOME_PERIODO'] = periodos['NOME_PERIODO'].str.split(' ', expand = True)[0]
id_periodos = periodos['NOME_PERIODO'].to_frame()

# DataFrame :: Chaves identificadores dos Portos
# DataFrame :: portos :: dado primário
portos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['portos']), 
                      sheet_name = arquivos_primarios['portos'].split('.')[0],
                      usecols = list(tp_dado_arquivos['portos'].keys()),
                      dtype = tp_dado_arquivos['portos']).applymap(fx.padronizar)

# DataFrame :: portos_correntes :: granularidade de correntes
portos_correntes = portos.copy()

# DataFrame :: az_portos :: unidades de armazenagem dos portos
az_portos = portos.copy()
az_portos = portos[['NOME_AZ_PORTO_VCM','PORTO']].drop_duplicates()

# DataFrame :: postos :: apenas a nível de PORTO e NOME_PORTO_VCM
portos = portos[['NOME_PORTO_VCM','PORTO']].drop_duplicates()

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
# (19/08/2025) Pegando aqui os produtos que não existem de fato.
produtos_inexistentes = cadastro_produtos['PRD-VCM'].loc[cadastro_produtos['CODIGO_ITEM'].str.startswith(('MP', 'PF'))].to_list()
# (19/08/2025) Filtrando os códigos que não (~) começam com 'MP' ou 'PF', para pegar apenas produtos que existem de fato.
cadastro_produtos = cadastro_produtos.loc[~cadastro_produtos['CODIGO_ITEM'].str.startswith(('MP', 'PF'))]

# DataFrame :: cadastro de matérias-primas :: filtro no tipo de material da tabela CADASTRO
cadastro_mp = cadastro_produtos[(cadastro_produtos['TIPO_MATERIAL'].str.split('-',expand=True)[0].str.strip() == 'MP')]

# DataFrame :: agrupamento de materiais :: busca todo o de-para de códigos específicos em códigos agrupados
agrupamento_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                            sheet_name = arquivos_primarios['cadastro_produtos_sn02'],
                            usecols = list(tp_dado_arquivos['cadastro_produtos_sn02'].keys()),
                            dtype = tp_dado_arquivos['cadastro_produtos_sn02']).applymap(fx.padronizar)
agrupamento_produtos = agrupamento_produtos[agrupamento_produtos['TIPO_MATERIAL'] == 'MP']
proxy_agrupamento = cadastro_mp[['CODIGO_ITEM','DESCRICAO']]
proxy_agrupamento = proxy_agrupamento.rename(columns={'CODIGO_ITEM':'COD_ESPECIFICO','DESCRICAO':'DESCRICAO_ESPECIFICA'})
proxy_agrupamento['CODIGO_AGRUPADO'] = proxy_agrupamento['COD_ESPECIFICO']
proxy_agrupamento['AGRUPAMENTO_MP'] = proxy_agrupamento['DESCRICAO_ESPECIFICA']
agrupamento_produtos = pd.concat([agrupamento_produtos,proxy_agrupamento])
agrupamento_produtos = agrupamento_produtos.drop_duplicates(subset = 'COD_ESPECIFICO')
agrupamento_produtos = agrupamento_produtos.drop(columns='TIPO_MATERIAL')

# DataFrame :: template do Suprimento Faixa :: arquivo de esqueleto topológico a partir do VCM
wizard_suprimento_faixa = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['wizard_suprimento_faixa']),
                                        sheet_name = arquivos_primarios['wizard_suprimento_faixa_sn01'],
                                        usecols = list(tp_dado_arquivos['wizard_suprimento_faixa'].keys()),
                                        dtype = tp_dado_arquivos['wizard_suprimento_faixa'])

# DataFrame :: dado primário de capacidade portuária
capacidade_portos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['capacidade_portos']),
                                  sheet_name = arquivos_primarios['capacidade_portos'].split('.')[0],
                                  # uselcols = omitido por conta da consulta dinâmica de horizonte de portos
                                  dtype = tp_dado_arquivos['capacidade_portos']).applymap(fx.padronizar)

# DataFrame :: capacidades operacionais do porto :: inclui dados de leadtime
# Obs - potencial para substituir iptCapacidadePortuaria.xlsx preenchido pelo S&OP
# com uma fonte que parte diretamente do time de logística

cap_op_portos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cap_op_portos']),
                              sheet_name = arquivos_primarios['cap_op_portos'].split('.')[0],
                              # uselcols omitido para permitir a captura dinâmica de colunas
                              dtype = tp_dado_arquivos['cap_op_portos']).applymap(fx.padronizar)

# DataFrame :: Conjunto de Correntes :: Esqueleto Primário da Topologia VCM
correntes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['correntes']),
                          sheet_name = arquivos_primarios['correntes'].split('.')[0],
                          usecols = list(tp_dado_arquivos['correntes'].keys()),
                          dtype = tp_dado_arquivos['correntes']).applymap(fx.padronizar)

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
df_revisao_importada = fx.left_outer_join(df_revisao_importada,agrupamento_produtos,left_on='CODIGO_MP',right_on='COD_ESPECIFICO',
                       name_left='Revisão de Chegadas >>Importada<<', name_right='Agrupamento de Produtos')
df_revisao_importada = fx.left_outer_join(df_revisao_importada, periodos, left_on = 'DT_REMESSA', right_on = 'PERIODO',
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
df_revisao_nacional = fx.left_outer_join(df_revisao_nacional, periodos, left_on = 'PROXY_PERIODO', right_on = 'pk_NOME_PERIODO',
                      name_left='Revisão de Chegadas >>Nacional<<',name_right='Períodos')
df_revisao_nacional = fx.left_outer_join(df_revisao_nacional, cadastro_mp, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM',
                      name_left='Revisão de Chegadas >>Nacional<<',name_right='Cadstro de Produtos VCM')

# 3. DataFrame de Compras Completo :: Importado + Nacional
cols = ['PORTO','PLANTA','MP','COMPANY','CODIGO_MP','COD_ESPECIFICO','CODIGO_AGRUPADO','PERIODO','NOME_PERIODO','PRD-VCM','DESCRICAO','TIPO_MATERIAL','CATEGORIA','BALANCE_TONS']
df_revisao_importada = df_revisao_importada[cols]
df_revisao_nacional = df_revisao_nacional[cols]
df_revisao = pd.concat([df_revisao_importada,df_revisao_nacional])
df_revisao = df_revisao.reset_index().drop(columns='index')
df_revisao = fx.left_outer_join(df_revisao, portos, left_on = 'PORTO', right_on = 'PORTO',
             name_left='Revisão de Chegadas >>ALL<<', name_right='Portos')

# Salvando um dataframe com o histórico da execução para log_futuro
exec_hist_df_revisao = df_revisao.copy()

print('Agrupando dados...')
df_revisao = df_revisao.groupby(by = ['NOME_PORTO_VCM','PRD-VCM','NOME_PERIODO'])['BALANCE_TONS'].sum().reset_index()
df_revisao['pk_right'] = df_revisao['NOME_PORTO_VCM'] + '-' + df_revisao['PRD-VCM'] + '-' + df_revisao['NOME_PERIODO']
print('Inserindo dados na estrutura topológica...')
wizard_suprimento_faixa['Suprimento Mínimo'] = 0.0
wizard_suprimento_faixa['Suprimento Máximo'] = 0.0
wizard_suprimento_faixa['pk_left'] = wizard_suprimento_faixa['Unidade'] + '-' + wizard_suprimento_faixa['Produto'] + '-' + wizard_suprimento_faixa['Periodo']
wizard_suprimento_faixa = fx.left_outer_join(wizard_suprimento_faixa,df_revisao, left_on = 'pk_left', right_on = 'pk_right',
                          name_left='Template Suprimento Faixa', name_right='Revisão de Chegadas')
suprimento_cmiss = suprimento_cmiss[(suprimento_cmiss['Indicador 2'] == 'B2B')].reset_index().drop(columns='index')
suprimento_cmiss = fx.left_outer_join(suprimento_cmiss, produtos_cmiss[['PRD-VCM','ITEM_CODE']], 
                                      left_on = 'Produto-VCM', right_on = 'PRD-VCM',
                                      name_left = 'Suprimento Atendido CMISS', name_right = 'Produtos VCM')
suprimento_cmiss = fx.left_outer_join(suprimento_cmiss, agrupamento_produtos,
                                      left_on = 'ITEM_CODE', right_on = 'COD_ESPECIFICO',
                                      name_left = 'Suprimento Atendido CMISS',
                                      name_right = 'Agrupamento de Produtos')
suprimento_cmiss = fx.left_outer_join(suprimento_cmiss, cadastro_mp[['CODIGO_ITEM','PRD-VCM']],
                                      left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM',
                                      name_left = 'Suprimento Atendido CMISS',
                                      name_right = 'Cadastro VCM')
suprimento_cmiss = fx.left_outer_join(suprimento_cmiss, periodos, left_on = 'Período', right_on = 'PERIODO',
                   name_left = 'Suprimento Atendido CMISS', name_right = 'Períodos')
unidades_for = {
    'FOR-NAC-PNA':'MC-PNO',
    'FOR-NAC-RND':'MC-RNO',
    'FOR-NAC-SPA':'MC-SPA',
    'FOR-NAC-SNO':'MC-SNO',
    'FOR-NAC-ARA':'MC-ARO',
    'FOR-NAC-CTA':'MC-CTO',
    'FOR-NAC-QRE':'MC-QRO',
    'FOR-NAC-BCA':'MC-BCO',
    'FOR-NAC-SLU':'MC-SLO'
}
suprimento_cmiss = suprimento_cmiss.replace(unidades_for.values(),unidades_for.keys())
suprimento_cmiss = suprimento_cmiss[['Unidade-Destino-VCM','PRD-VCM_y','NOME_PERIODO','Quantidade']]
suprimento_cmiss = suprimento_cmiss.rename(columns = {'Unidade-Destino-VCM':'Unidade','PRD-VCM_y':'Produto','NOME_PERIODO':'Periodo'})
suprimento_cmiss = suprimento_cmiss.groupby(by=['Unidade','Produto','Periodo'])['Quantidade'].sum().reset_index()
wizard_suprimento_faixa = fx.left_outer_join(wizard_suprimento_faixa, suprimento_cmiss,
                                             left_on = ['Unidade','Produto','Periodo'],
                                             right_on = ['Unidade','Produto','Periodo'],
                                             name_left = 'Template Suprimento', name_right = 'Suprimento Atendido CMISS')
wizard_suprimento_faixa['Quantidade'] = wizard_suprimento_faixa['Quantidade'].fillna(0.0)
wizard_suprimento_faixa['BALANCE_TONS'] = wizard_suprimento_faixa['BALANCE_TONS'].fillna(0.0)
wizard_suprimento_faixa['BALANCE_TONS'] = wizard_suprimento_faixa['BALANCE_TONS'] + wizard_suprimento_faixa['Quantidade']

print('\nAplicando premissas para compras firmes...')
print(' >> Horizonte Compras Importadas: M+0 até M+2')
print(' >> Horizonte Compras Nacionais: M+0 até M+1')
purchase_range = ['Mês000','Mês001','Mês002']
purchase_range_nac = ['Mês000','Mês001']
mp_list_nac = list(mp_fornecimento_nacional['PRD-VCM-NAC'])
for i in tqdm(range(wizard_suprimento_faixa.shape[0]), desc = 'Processando...', unit = ' row'):
    # Caso 1: Está no horizonte de compra importada congelado e o fornecimento é importado
    if wizard_suprimento_faixa['Periodo'][i].split(' ')[0] in purchase_range \
       and wizard_suprimento_faixa['BALANCE_TONS'][i] > 0.0 \
       and wizard_suprimento_faixa['Unidade'][i][:3] == 'POR':
        wizard_suprimento_faixa['Suprimento Mínimo'][i] = wizard_suprimento_faixa['BALANCE_TONS'][i]
        wizard_suprimento_faixa['Suprimento Máximo'][i] = wizard_suprimento_faixa['Suprimento Mínimo'][i]
    
    # Caso 2: Não está no horizonte de fornecimento importado congelado e o fornecimento é importado
    elif wizard_suprimento_faixa['Periodo'][i].split(' ')[0] not in purchase_range \
       and wizard_suprimento_faixa['Unidade'][i][:3] == 'POR' \
       and wizard_suprimento_faixa['Produto'][i] not in produtos_inexistentes:
        wizard_suprimento_faixa['Suprimento Mínimo'][i] = wizard_suprimento_faixa['BALANCE_TONS'][i]
        wizard_suprimento_faixa['Suprimento Máximo'][i] = 100000.0
    
    # Caso 3: Para os casos de fornecimento nacional
    elif wizard_suprimento_faixa['Unidade'][i][:7] == 'FOR-NAC':
        # Caso 3.1: Suprimento mínimo supera o threshold de 10 kton
        if wizard_suprimento_faixa['BALANCE_TONS'][i] > 10000.0:
            wizard_suprimento_faixa['Suprimento Mínimo'][i] = wizard_suprimento_faixa['BALANCE_TONS'][i]
            wizard_suprimento_faixa['Suprimento Máximo'][i] = wizard_suprimento_faixa['Suprimento Mínimo'][i]
        # Caso 3.2: Por notar que a otimização sugeria com muita facilidade compras no período de 000 a 001,
        # uma nova premissa foi estabelecida em 29/05/2023 - Horizonte de Fornecimento Nacional Congelado
        elif wizard_suprimento_faixa['Periodo'][i].split(' ')[0] in purchase_range_nac:
            wizard_suprimento_faixa['Suprimento Mínimo'][i] = wizard_suprimento_faixa['BALANCE_TONS'][i]
            wizard_suprimento_faixa['Suprimento Máximo'][i] = wizard_suprimento_faixa['Suprimento Mínimo'][i]
        # Caso 3.3: Fornecimento Nacional está fora do Horizonte de Fornecimento Nacional Congelado
        elif wizard_suprimento_faixa['Produto'][i] in mp_list_nac:
            wizard_suprimento_faixa['Suprimento Mínimo'][i] = wizard_suprimento_faixa['BALANCE_TONS'][i]
            wizard_suprimento_faixa['Suprimento Máximo'][i] = 100000.0
        # Caso 3.4: Material não consta na lista de MPs adquiridas nacionalmente
        else:
            wizard_suprimento_faixa['Suprimento Mínimo'][i] = wizard_suprimento_faixa['BALANCE_TONS'][i]    
            # wizard_suprimento_faixa['Suprimento Máximo'][i] = 0.0
            wizard_suprimento_faixa['Suprimento Máximo'][i] = wizard_suprimento_faixa['Suprimento Mínimo'][i]

columns = ['Unidade','Produto','Periodo','Suprimento Mínimo','Suprimento Máximo']
wizard_suprimento_faixa = wizard_suprimento_faixa[columns]
wizard_suprimento_faixa = wizard_suprimento_faixa.fillna(0.0)
wizard_suprimento_faixa = wizard_suprimento_faixa.round({'Suprimento Mínimo':2,'Suprimento Máximo':2}) # arredonda com duas casas decimais
wizard_suprimento_faixa.to_excel(os.path.join(cwd,output_path+'tbOutSuprimentoFaixa.xlsx'),sheet_name='SUPRIMENTO_FAIXA',index = False)
print('Arquivo tbOutSuprimentoFaixa.xlsx foi Atualizado com Sucesso!')
# =============================================================================================================
# 2025-05-30 :: LINHAS DEPRECADAS porque as capacidades portuárias serão atualizadas pelo script de limites
# =============================================================================================================
# print('\n')
# print('╔════════════════════════════════════════════════════════════════════╗')
# print('║                    >>  CAPACIDADE PORTUÁRIA  <<                    ║')
# print('╠════════════════════════════════════════════════════════════════════╣')
# print('║ # WIZARD_CAPACIDADE_PORTOS :: Atualiza a capacidade dos portos     ║')
# print('╚════════════════════════════════════════════════════════════════════╝')
# print('\n')
# print('Iniciando...')
# # 4. DataFrame de Capacidade Portuária :: APO
# capacidade_portos = capacidade_portos.melt(id_vars = ['PERIODO'],
#                                            var_name = 'PORTO',
#                                            value_name = 'CAPACIDADE')
# capacidade_portos = capacidade_portos.applymap(fx.padronizar)
# fx.left_outer_join(capacidade_portos, periodos, left_on = 'PERIODO', right_on = 'PERIODO')
# fx.left_outer_join(capacidade_portos, az_portos, left_on = 'PORTO', right_on = 'PORTO')
# # Trocando a base de kton para ton
# capacidade_portos['CAPACIDADE'] = capacidade_portos['CAPACIDADE'].apply(lambda x: x*1000)
# capacidade_portos = capacidade_portos.rename(columns={'CAPACIDADE':'Limite','NOME_AZ_PORTO_VCM':'Unidade','PERIODO':'Periodo'})
# wizard_capacidade_portos = capacidade_portos
# wizard_capacidade_portos = wizard_capacidade_portos[['Unidade','Periodo','Limite']]
# wizard_capacidade_portos['Ativo'] = 'True'
# wizard_capacidade_portos = wizard_capacidade_portos.round({'Limite':2})
# wizard_capacidade_portos.to_csv(os.path.join(cwd, output_path + 'tbOutCapProdPor_LimMaxS.csv'),
#                                 sep = ';', encoding = 'UTF-8-sig', index = False)
# print('Arquivo tbOutCapProdPor_LimMaxS.csv foi atualizado com sucesso!')

# =============================================================================================================
# 2025-05-30 :: LINHAS DEPRECADAS porque os leadtimes não estão ativados para execução no supply.py
# =============================================================================================================
# print('\n')
# print('╔════════════════════════════════════════════════════════════════════╗')
# print('║                         >>  LEADTIMES  <<                          ║')
# print('╠════════════════════════════════════════════════════════════════════╣')
# print('║ # WIZARD_LEADTIME :: Atualiza Leadtimes Inbound                    ║')
# print('╚════════════════════════════════════════════════════════════════════╝')
# print('\n')
# print('Iniciando...')
# # 5. DataFrame de Correntes :: Separando dois DataFrames para diferentes tratamentos
# df1_correntes = correntes.copy()
# df1_correntes = df1_correntes.rename(columns={'ConjuntoCorrentes':'Corrente',
#                                               'Unidade-Origem':'Origem',
#                                               'Unidade-Destino':'Destino'})
# df2_correntes = df1_correntes.copy()
# df2_correntes = df2_correntes.loc[df2_correntes['Tipo'] == 'PORTO',:].reset_index().drop(columns='index')
# df2_correntes['Porto'] = df2_correntes['Destino'].str.split('-').str[1]
# df2_correntes['proxy'] = df2_correntes['Porto']

# df1_correntes = df1_correntes.loc[df1_correntes['Tipo'] == 'INBOUND',:].reset_index().drop(columns='index')
# df1_correntes['Porto'] = df1_correntes['Origem'].str.split('-').str[1]
# df1_correntes['Unidade'] = df1_correntes['Origem'].str.split('-').str[2]
# df1_correntes['proxy'] = df1_correntes['Porto'] + '-' + df1_correntes['Unidade']

# # Tratando o DataFrame de portos especialmente para Leadtimes
# df_portos = portos.copy()
# df_portos = df_portos.loc[df_portos['NOME_PORTO_VCM'].str.split('-').str[1] != 'NAC',:]
# df_portos['PROXY'] = df_portos['NOME_PORTO_VCM'].str.split('-').str[1]

# # 6. DataFrame de Capacidades Operacionais dos Portos :: Separando dados referentes aos leadtimes
# df_database = cap_op_portos.copy()
# cols = df_database.columns[4:]
# df_database[cols] = df_database[cols].apply(pd.to_numeric, errors = 'coerce')
# df_database = pd.melt(df_database, id_vars = ['Porto','Origem','Destino','Variável'],
#                       var_name = 'Periodo',
#                       value_name = 'Leadtime')
# df_database['Periodo'] = pd.to_datetime(df_database['Periodo'])
# df_database['Leadtime'] = df_database['Leadtime'].fillna(0.0)
# df_database = df_database.astype({'Leadtime':'int'})
# # Separaremos os leadtimes em duas etapas: 1) Waiting Time 2) Fila Ferroviária + Outros
# df_database_fila = df_database.loc[(df_database['Variável'] == 'FILA PORTUARIA')\
#                                    |(df_database['Variável'] == 'FILA FERROVIARIA')]
#                                    # 13/03/2024: Excluindo condições abaixo
#                                    #|(df_database['Variável'] == 'Transbordo AZ')]
# # Pivotando a tabela para criar uma tabela resumida
# df_database_fila = df_database_fila.pivot(index = ['Porto','Origem','Destino','Periodo'],
#                                           columns = 'Variável',
#                                           values='Leadtime')
# # Preencher NaN com zeros, isto é, se não há informação o leadtime adicionar é 0
# df_database_fila = df_database_fila.reset_index().fillna(0.0)

# df_database_fila = df_database_fila[['Porto','Periodo','FILA PORTUARIA','FILA FERROVIARIA']]
# # Criar um outro dataframe com os demais leadtimes
# df_leadtime = df_database.loc[(df_database['Variável'] != 'FILA PORTUARIA')\
#                                &(df_database['Variável'] != 'FILA FERROVIARIA')]
#                                # 13/03/2024: Não avaliar as condições abaixo
#                                #&(df_database['Variável'] != 'Transbordo AZ')]
# # Capturando os pontos de transbordo existentes: 1) Miritituba, 2) Palmeirante e 3) Rio Verde
# df_leadtime_transbordo = df_leadtime.loc[(df_leadtime['Destino'] == 'MIRITITUBA')\
#                                          |(df_leadtime['Destino'] == 'PALMEIRANTE')\
#                                          |(df_leadtime['Destino'] == 'RIO VERDE'),:]\
#                                     .reset_index().drop(columns = 'index')
# # Excluindo do dataframe leadtime os pontos de transbordo
# df_leadtime = df_leadtime.loc[(df_leadtime['Destino'] != 'MIRITITUBA')\
#                               &(df_leadtime['Destino'] != 'PALMEIRANTE')\
#                               &(df_leadtime['Destino'] != 'RIO VERDE'),:]\
#                          .reset_index().drop(columns = 'index')
# # Pivotando todos os leadtimes em formato de tabela
# df_leadtime = df_leadtime.pivot(index = ['Porto','Origem','Destino','Periodo'],
#                                           columns = 'Variável',
#                                           values='Leadtime')

# df_leadtime = df_leadtime.reset_index().fillna(0.0)
# df_leadtime.columns.name = None
# fx.left_outer_join(df_leadtime, df_database_fila, left_on = ['Porto','Periodo'], right_on = ['Porto','Periodo'])
# df_leadtime['TRANSBORDO'] = 0.0
# for i in tqdm(range(df_leadtime.shape[0]), desc = 'Preenchendo Leadtimes...'):
#     for j in range(df_leadtime_transbordo.shape[0]):
#         if df_leadtime['Porto'][i] == df_leadtime_transbordo['Porto'][j] \
#         and df_leadtime['Origem'][i] == df_leadtime_transbordo['Destino'][j]\
#             and df_leadtime['Periodo'][i] == df_leadtime_transbordo['Periodo'][j]:
#             df_leadtime['TRANSBORDO'][i] = df_leadtime_transbordo['Leadtime'][j]
# print('Exportando uma base de dados para consulta..')
# df_leadtime.to_excel(os.path.join(cwd, output_path + 'LEADTIMES.xlsx'), index = False)
# df_leadtime['TRANSITO'] = df_leadtime['MULTIMODAL - TRANSITO FERRO'] + \
#                           df_leadtime['MULTIMODAL - TRANSITO RODO'] + \
#                           df_leadtime['TRANSITO RODO'] + \
#                           df_leadtime['TRANSBORDO AZ'] + \
#                           df_leadtime['TRANSBORDO'] + \
#                           df_leadtime['FILA FERROVIARIA']
# df_leadtime = df_leadtime[['Porto','Origem','Destino','Periodo','FILA PORTUARIA','TRANSITO']]
# fx.left_outer_join(df_leadtime, df_portos, left_on = 'Porto', right_on = 'PORTO')
# from_rep = ['QUERENCIA','ARAGUARI','CATALAO','PORTO NACIONAL','PAULINIA',
#             'SINOP','RONDONOPOLIS','LEM', 'SAO LUIS']

# to_rep = ['LTQRE','LTARA','LTCTA','LTPNA','LTSPA',
#           'LTSNO','LTRND','LTLMA','LTSLU']
# df_leadtime['Destino'] = df_leadtime['Destino'].replace(from_rep,to_rep)
# df_leadtime['PROXY_2'] = df_leadtime['PROXY'].astype('str') + '-' + df_leadtime['Destino'].astype('str')
# df_leadtime_fila = df_leadtime.groupby(['Periodo','PROXY'],\
#                                          as_index = False).agg({'FILA PORTUARIA':'mean'})

# df_leadtime_transt = df_leadtime.groupby(['Periodo','PROXY_2'],\
#                                          as_index = False).agg({'TRANSITO':'mean'})
# # Expandindo os dataframes de correntes em função do período
# df1_correntes = df1_correntes.merge(periodos, how = 'cross')
# df2_correntes = df2_correntes.merge(periodos, how = 'cross')
# fx.left_outer_join(df1_correntes, df_leadtime_transt, left_on = ['PERIODO','proxy'], right_on = ['Periodo','PROXY_2'] )
# fx.left_outer_join(df2_correntes, df_leadtime_fila, left_on = ['PERIODO','proxy'], right_on = ['Periodo','PROXY'])
# df1_correntes = df1_correntes.drop(columns = 'Periodo').rename(columns = {'TRANSITO':'Valor','NOME_PERIODO':'Periodo'})
# df2_correntes = df2_correntes.drop(columns = 'Periodo').rename(columns = {'FILA PORTUARIA':'Valor','NOME_PERIODO':'Periodo'})
# wizard_leadtime = pd.concat([df1_correntes, df2_correntes])[['Corrente','Periodo','Valor']]
# wizard_leadtime['Valor'] = wizard_leadtime['Valor'].div(30)

# # Aplicando a função e ajustando os leadtimes
# wizard_leadtime['Valor'] = wizard_leadtime['Valor'].apply(fx.custom_round)
# #wizard_leadtime.to_excel(os.path.join(cwd,output_path+'tbOutLeadTimes.xlsx'),sheet_name='Leadtimes',index = False)
# print('Arquivo WIZARD_LEADTIMES.xlsx calculado, mas não preenchido!')
end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')