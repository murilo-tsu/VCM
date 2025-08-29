print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                         ATUALIZACAO DE DADOS - VCM                                             ║')
print('║                                            >> yield_deploy.py <<                                               ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado  por: Murilo Lima Ribeiro  Data: 02/04/2025                                                             ║')
print('║ Editado por: Murilo Lima Ribeiro  Data: 29/08/2025                                                             ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v2.0.0 (25/08/2025): Release Projeto Merger                                                                  ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Lista Técnica VCM                                                                                           ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
print('\n')

# =======================================================================================================================
# IMPORTAR BIBLIOTECAS
# =======================================================================================================================

import os
import sys
import math
import pandas as pd
import numpy as np
import time
import datetime
import warnings
import logging
import inspect
from decimal import Decimal
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import MonthBegin
from tkinter import messagebox
from unidecode import unidecode
from tqdm import tqdm
 
pd.options.mode.chained_assignment = None  # default='warn'
warnings.filterwarnings('ignore')

# =======================================================================================================================
# CONFIGURAÇÕES INICIAIS
# =======================================================================================================================

start_time = time.time()
cwd = os.getcwd()
# INPUT DATA - pasta com dados de entrada referentes ao ciclo de planejamento vigente
# Rotina de Atualização: mensal ou conforme necessidade do ciclo
path = 'Input Data/'

# OUTPUT DATA - pasta com dados de saída do script de atualização
# Rotina de Atualização: conforme a execução de scripts
# Os dados nessa pasta se tornam input para o VCM
output_path = 'Output Data/'

# ERROR LOGS - pasta com registros de erros durante a execução do script
# Rotina de Atualização: conforme execução de scripts
# Os arquivos servem para identificação de problemas nos inputs ou topologia
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

# DataFrame ::  Dicionário Genérico
dicgen = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['dicgen']),
                       sheet_name = arquivos_primarios['dicgen'].split('.')[0],
                       usecols = list(tp_dado_arquivos['dicgen'].keys()),
                       dtype = tp_dado_arquivos['dicgen'])

# DataFrame :: Lista Técnica
bom = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['bom']),
                    sheet_name = arquivos_primarios['bom'].split('.')[0],
                    usecols = list(tp_dado_arquivos['bom'].keys()),
                    dtype = tp_dado_arquivos['bom'])

bom['PRODUCTION_SITE'] = np.where(bom['PRODUCTION_SITE'] == '1001',
                                  bom['PLANT_CODE'],
                                  bom['PRODUCTION_SITE'])

bom['COMPONENT_QTY'] = np.where(bom['COMPANY_CODE'] == 'ECFHG',
                                bom['COMPONENT_QTY']/1000,
                                bom['COMPONENT_QTY'])

bom['ID'] = bom['PLANT_CODE'] + '-' + bom['PRODUCTION_SITE'] + '-' + bom['FG_CODE']
bom['COMPONENT_QTY'] = bom['COMPONENT_QTY'].apply(lambda x: round(x,3))
bom['UOM_FORMULA'] = np.where(bom['UOM_FORMULA'] == 'TO','TN',bom['UOM_FORMULA'])
bom = bom[(bom['UOM_FORMULA'] == 'TN')]
bom['FORMULA_CODE'] = bom['ID']

# DataFrame :: Contagem de componentes na Lista Técnica
bom_comp_count = bom.copy()
bom_comp_count = bom_comp_count.groupby(by = ['ID','PLANT_CODE','PRODUCTION_SITE','FG_CODE'])['RM_CODE'].count().reset_index()
bom_comp_count = bom_comp_count.rename(columns={'RM_CODE':'COUNT'})
proxy_comp_count = bom_comp_count.copy()
proxy_comp_count = proxy_comp_count.rename(columns={'PLANT_CODE':'PLANT_CODE_PC','PRODUCTION_SITE':'PRODUCTION_SITE_PC',
                                                    'FG_CODE':'FG_CODE_PC'})

# DataFrame :: Fechamento das Formulações da Lista Técnica
bom_comp_sum = bom.copy()
bom_comp_sum = bom_comp_sum.groupby(by = ['ID','PLANT_CODE','PRODUCTION_SITE','FG_CODE'])['COMPONENT_QTY'].sum().reset_index()
bom_comp_sum = bom_comp_sum.rename(columns = {'COMPONENT_QTY':'TOTAL_COMP'})
proxy_comp_sum = bom_comp_sum.copy()
proxy_comp_sum = proxy_comp_sum.rename(columns={'PLANT_CODE':'PLANT_CODE_PS','PRODUCTION_SITE':'PRODUCTION_SITE_PS','FG_CODE':'FG_CODE_PS'})


# DataFrame :: Criação de uma Lista Técnica Alternativa Irrestrita - Opção 02
# Apenas instaciando o DataFrame para uso posterior
bom_alt = pd.DataFrame()

# DataFrame :: cadastro de materiais :: busca toda a lista de materiais (MP, PI, PF) no cadastrados VCM
cadastro_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                            sheet_name = arquivos_primarios['cadastro_produtos_sn01'],
                            usecols = list(tp_dado_arquivos['cadastro_produtos_sn01'].keys()),
                            dtype = tp_dado_arquivos['cadastro_produtos_sn01']).applymap(fx.padronizar)

# DataFrame :: cadastro de matérias-primas :: filtro no tipo de material da tabela CADASTRO
cadastro_mp = cadastro_produtos[(cadastro_produtos['TIPO_MATERIAL'].str.split('-',expand=True)[0].str.strip() == 'MP')]
cadastro_pf = cadastro_produtos[(cadastro_produtos['TIPO_MATERIAL'].str.split('-',expand=True)[0].str.strip() == 'PF')]

# DataFrame :: agrupamento de materiais :: busca todo o de-para de códigos específicos em códigos agrupados
agrupamento_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                            sheet_name = arquivos_primarios['cadastro_produtos_sn02'],
                            usecols = list(tp_dado_arquivos['cadastro_produtos_sn02'].keys()),
                            dtype = tp_dado_arquivos['cadastro_produtos_sn02']).applymap(fx.padronizar)

# Agrupamento de Produtos Acabados
agrupamento_produtos_pf = agrupamento_produtos.copy()
agrupamento_produtos_pf = agrupamento_produtos_pf[agrupamento_produtos_pf['TIPO_MATERIAL'] == 'PF']
proxy_agrupamento_pf = cadastro_pf[['CODIGO_ITEM','DESCRICAO']]
proxy_agrupamento_pf = proxy_agrupamento_pf.rename(columns={'CODIGO_ITEM':'COD_ESPECIFICO','DESCRICAO':'DESCRICAO_ESPECIFICA'})
proxy_agrupamento_pf['CODIGO_AGRUPADO'] = proxy_agrupamento_pf['COD_ESPECIFICO']
proxy_agrupamento_pf['AGRUPAMENTO'] = proxy_agrupamento_pf['DESCRICAO_ESPECIFICA']
agrupamento_produtos_pf = pd.concat([agrupamento_produtos_pf,proxy_agrupamento_pf])
agrupamento_produtos_pf = agrupamento_produtos_pf.drop_duplicates(subset = 'COD_ESPECIFICO')

# Agrupamento de Matérias-Primas
agrupamento_produtos_mp = agrupamento_produtos.copy()
agrupamento_produtos_mp = agrupamento_produtos_mp[agrupamento_produtos_mp['TIPO_MATERIAL'] == 'MP']
proxy_agrupamento_mp = cadastro_mp[['CODIGO_ITEM','DESCRICAO']]
proxy_agrupamento_mp = proxy_agrupamento_mp.rename(columns={'CODIGO_ITEM':'COD_ESPECIFICO','DESCRICAO':'DESCRICAO_ESPECIFICA'})
proxy_agrupamento_mp['CODIGO_AGRUPADO'] = proxy_agrupamento_mp['COD_ESPECIFICO']
proxy_agrupamento_mp['AGRUPAMENTO'] = proxy_agrupamento_mp['DESCRICAO_ESPECIFICA']
agrupamento_produtos_mp = pd.concat([agrupamento_produtos_mp,proxy_agrupamento_mp])
agrupamento_produtos_mp = agrupamento_produtos_mp.drop_duplicates(subset = 'COD_ESPECIFICO')

# DataFrame :: Unidades Produtoras relevantes para lista técnica
unidades_produtoras = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_exp']),
                                    sheet_name = arquivos_primarios['unidades_exp'].split('.')[0],
                                    usecols = list(tp_dado_arquivos['unidades_exp'].keys()),
                                    dtype = tp_dado_arquivos['unidades_exp'])


# ================================================= DEPRECADO ============================================================
# 2025-06-06 :: Etapa deprecada porque com a nova atualização das bases de dados,
# a informação que se pretende buscar encontra-se disponível imediatamente no de-para
#unidades_produtoras = unidades_produtoras.melt(id_vars = list(unidades_produtoras.columns[:-1]),
#                                               var_name = 'TIPO_UNIDADE_VCM', value_name = 'UNIDADE_VCM',
#                                               value_vars = list(unidades_produtoras.columns[-1:]))
# Alterar o nome da coluna UP_MISTURADORA_VCM para reduzir as alterações de código e receber o nome
# de variável anteriormente fornecido pelo método .melt
# ========================================================================================================================


unidades_produtoras = unidades_produtoras.rename(columns={'UP_MISTURADORA_VCM':'UNIDADE_VCM'})
unidades_produtoras = unidades_produtoras[(unidades_produtoras['UNIDADE_VCM'].notna())]
unidades_produtoras['DEPOSITO'] = np.where(unidades_produtoras['DEPOSITO'] == '1001',
                                           unidades_produtoras['PLANTA'],
                                           unidades_produtoras['DEPOSITO'])

# DataFrame :: Unidades Produtoras por Gerência/Consultoria de Vendas
unidades_terc = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_terceiras']),
                              usecols = list(tp_dado_arquivos['unidades_terceiras'].keys()),
                              dtype = tp_dado_arquivos['unidades_terceiras'])

unidades_terc['UNIDADE PRODUTORA'] = unidades_terc['UNIDADE PRODUTORA'].replace(list(dicgen['DE']),list(dicgen['PARA']))
unidades_terc['UNIDADE FATURAMENTO'] = unidades_terc['UNIDADE FATURAMENTO'].replace(list(dicgen['DE']),list(dicgen['PARA']))

# DataFrame ::  Arquivo de Demanda Irrestrita
demanda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['demanda']),
                        sheet_name = arquivos_primarios['demanda_sn'],
                        usecols = list(tp_dado_arquivos['demanda'].keys()),
                        dtype = tp_dado_arquivos['demanda'])

demanda['UNIDADE PRODUTORA'] = demanda['UNIDADE PRODUTORA'].replace(list(dicgen['DE']),list(dicgen['PARA']))

# DataFrame :: Template de Rendimento de Receitas Saída
#fx.validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_RendSaida']))
# template_saida = pd.read_excel(os.path.join(cwd,path + arquivos_primarios['template_RendSaida']),
#                                 sheet_name = arquivos_primarios['template_RendSaida_sn01'],
#                                 usecols=list(tp_dado_arquivos['template_RendSaida'].keys()),
#                                 dtype = tp_dado_arquivos['template_RendSaida'])

template_saida = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_RendSaida']),
                    usecols = list(tp_dado_arquivos['template_RendSaida'].keys()),
                    dtype = tp_dado_arquivos['template_RendSaida'], sep = ';')

# DataFrame :: Template de Rendimento de Receitas de Entrada
#fx.validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_RendEntr']))
# template_entrada = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_RendEntr']),
#                                  sheet_name = arquivos_primarios['template_RendEntr_sn01'],
#                                  usecols = list(tp_dado_arquivos['template_RendEntr'].keys()),
#                                  dtype = tp_dado_arquivos['template_RendEntr'])

template_entrada = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_RendEntr']),
                    usecols = list(tp_dado_arquivos['template_RendEntr'].keys()),
                    dtype = tp_dado_arquivos['template_RendEntr'], sep = ';')

# DataFrame :: Criação de uma Lista Técnica Alternativa com  base em preenchimentos anteriores VCM - Opção 03
bom_alt_vcm = template_saida.copy()
bom_alt_vcm = bom_alt_vcm[(bom_alt_vcm['ValorSaida'] == 1.0)].reset_index().drop(columns='index')
bom_alt_vcm_mp = template_entrada.copy()
bom_alt_vcm_mp = bom_alt_vcm_mp[(bom_alt_vcm_mp['ValorEntrada'] > 0.0)].reset_index().drop(columns='index')
bom_alt_vcm_mp = bom_alt_vcm_mp.rename(columns={'Produto':'MP'})

# =======================================================================================================================
# CORE DO PROCESSAMENTO DE DADOS
# =======================================================================================================================

print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                         >>  EXPLOSÃO DA LISTA TÉCNICA  <<                                      ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ # WIZARD_RENDIMENTO_ENTRADA :: Receitas por MP                                                                 ║')
print('║ # WIZARD_RENDIMENTO_SAIDA   :: Receitas por PF                                                                 ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
print('\n')
print('Iniciando...')
print('Etapa 01: Criando uma matriz identidade de receitas por PF')
template_saida['ValorSaida'] = 0.0
template_saida['Nro. Produto'] = template_saida['Produto'].str[2:]
template_saida['Nro. Produto'] = template_saida['Nro. Produto'].astype(np.int64)
template_saida['Nro. Receita'] = template_saida['Receita'].str[2:]
template_saida['Nro. Receita'] = template_saida['Nro. Receita'].astype(np.int64)
template_saida = template_saida.sort_values(by=['Unidade','Nro. Produto','Nro. Receita'], ascending = True)
template_saida['Proxy MR'] = template_saida['Unidade'] + '-' + template_saida['Receita']
template_saida['Proxy PR'] = template_saida['Unidade'] + '-' + template_saida['Produto']
template_saida = template_saida.reset_index().drop(columns='index')
print('Iniciando looping para preencher matriz...')

# ================================================= DEPRECADO ============================================================
# for i in tqdm(range(template_saida.shape[0]), desc = 'Preenchimento Template de Saída'):
#     proxy_mr_find = template_saida['Proxy MR'][i]
#     proxy_pr_find = template_saida['Proxy PR'][i]
#     proxy_mr_look = template_saida.loc[template_saida.ValorSaida == 1,:]['Proxy MR'][:i].to_list()
#     proxy_pr_look = template_saida.loc[template_saida.ValorSaida == 1,:]['Proxy PR'][:i].to_list()
#     if proxy_mr_find not in proxy_mr_look and proxy_pr_find not in proxy_pr_look:
#          template_saida['ValorSaida'][i] = 1.0
               
#     else:
#          template_saida['ValorSaida'][i] = 0.0
# ================================================= DEPRECADO ============================================================

mr_values = template_saida['Proxy MR'].values
pr_values = template_saida['Proxy PR'].values
output = np.zeros(len(template_saida), dtype=float)

seen_mr = set()
seen_pr = set()

for i in tqdm(range(len(template_saida)), desc = 'Preenchimento Template de Saída'):
    if mr_values[i] not in seen_mr and pr_values[i] not in seen_pr:
        output[i] = 1.0
        seen_mr.add(mr_values[i])
        seen_pr.add(pr_values[i])

template_saida['ValorSaida'] = output

print('Matriz de Produtos Acabados preenchida com sucesso!')
cols_to_keep = ['Unidade','Receita','Produto','ValorSaida']
# template_saida[cols_to_keep].to_excel(os.path.join(cwd,output_path + 'tbOutRendimentosSaida.xlsx'), 
#                         index = False, sheet_name = 'RENDIMENTO_SAIDA_PROD')
template_saida[cols_to_keep].to_csv(os.path.join(cwd, output_path + 'tbOutRendimentoSai.csv'),
                                    index=False, sep = ';', encoding='utf-8')
template_saida_corr = template_saida.copy()
template_saida = template_saida[(template_saida['ValorSaida'] == 1.0)][['Proxy PR','Receita']].copy()

print('\nEstratégia 01 :: Avaliando balanceamento da lista técnica')
print('Contagem de componentes...')
bom = fx.left_outer_join(bom, proxy_comp_count, left_on = 'ID', right_on = 'ID',
                         name_left = 'Mesclagem (1) => BOM', name_right = 'Contagem de Componentes')
print('Fechamento das fórmulas...')
bom = fx.left_outer_join(bom, proxy_comp_sum, left_on = 'ID', right_on = 'ID',
                         name_left = 'Mesclagem (2) => BOM', name_right = 'Volume de Componentes')
# A regra para definir o balanceamento da fórmula pode ser inserido aqui
bom['STATUS'] = np.where(bom['TOTAL_COMP'] < 1.0,'Desbalanceada','Balanceada')
bom_alt = bom.copy()
bom = bom[(bom['STATUS'] == 'Balanceada')].copy()
proxy_bom = bom.copy()
proxy_bom['ID'] = proxy_bom['FG_CODE']
proxy_bom = proxy_bom.sort_values(by=["PLANT_CODE","PRODUCTION_SITE","FG_CODE","RM_CODE"])
pk_unique_fg_control = []
pk_rm_control = []
proxy_bom = proxy_bom.reset_index().drop(columns='index')
proxy_bom['pk_eval'] = False
for i in range(proxy_bom.shape[0]):
    if proxy_bom['ID'][i] in pk_unique_fg_control and proxy_bom['RM_CODE'][i] in pk_rm_control:
        proxy_bom['pk_eval'][i] = False
        pk_rm_control.append(proxy_bom['RM_CODE'][i])
    else:
        proxy_bom['pk_eval'][i] = True
        pk_unique_fg_control.append(proxy_bom['ID'][i])
        pk_rm_control = []

proxy_bom = proxy_bom[proxy_bom['pk_eval'] == True].copy()
proxy_bom = proxy_bom.groupby(by = ['FORMULA_CODE','FG_CODE','FINISHED_GOOD','RM_CODE','RM_DESCRIPTION'])['COMPONENT_QTY'].sum().reset_index()
proxy_bom['PRIORITY'] = [1 if proxy_bom['FORMULA_CODE'][i].split('-')[0] == proxy_bom['FORMULA_CODE'][i].split('-')[1] else 2 \
    for i in range(proxy_bom.shape[0])]
proxy_bom = proxy_bom.sort_values(by=['FG_CODE','FORMULA_CODE','PRIORITY'], ascending = False).reset_index().drop(columns='index')

def truncate(value, decimals=3):
    multiplier = 10 ** decimals
    return math.floor(value * multiplier) / multiplier

# Initialize
# proxy_bom['COMPONENT_QTY_TRUNC'] = proxy_bom['COMPONENT_QTY'].apply(lambda x: truncate(x, 3))
proxy_bom['COMPONENT_QTY_TRUNC'] = proxy_bom['COMPONENT_QTY'].apply(lambda x: round(x, 3))
fg_list = []
lt = False
fechamento = 0.0

for i in tqdm(range(proxy_bom.shape[0]), desc='Avaliação de fechamento da BOM'):
    current_qty = proxy_bom['COMPONENT_QTY_TRUNC'][i]
    current_fg = proxy_bom['FG_CODE'][i]
    sum_check = fechamento + current_qty
    
    if i == 0:
        proxy_bom.at[i, 'ACC_QTY'] = current_qty
        fechamento = current_qty
        fg_list.append(current_fg)
    
    elif current_fg not in fg_list:
        fg_list.append(current_fg)
        proxy_bom.at[i, 'ACC_QTY'] = current_qty
        fechamento = current_qty
        lt = False
    
    elif lt:
        proxy_bom.at[i, 'ACC_QTY'] = 0.0
    
    elif sum_check <= 1.0:
        proxy_bom.at[i, 'ACC_QTY'] = sum_check
        fechamento = sum_check
    
    else:
        needed = truncate(1.0 - fechamento, 3)
        proxy_bom.at[i, 'ACC_QTY'] = needed
        fechamento = 1.0  # Mark as complete
        lt = True  # Flag for subsequent components
        
        remaining = truncate(current_qty - needed, 3)
        if remaining > 0:
            # Inativado por enquanto
            pass

proxy_bom['COMPONENT_QTY'] = proxy_bom['COMPONENT_QTY_TRUNC']
proxy_bom = proxy_bom[(proxy_bom['ACC_QTY'] > 0.0)].copy()
proxy_bom['COMPONENT_QTY'] = proxy_bom['COMPONENT_QTY'].round(3)
plants = pd.DataFrame(list(bom_alt['PLANT_CODE'].unique())).rename(columns={0:'PLANT_CODE'})
# production_sites = pd.DataFrame(list(demanda['UNIDADE PRODUTORA'].unique())).rename(columns={0:'PRODUCTION_SITE'})
production_sites = pd.DataFrame(list(bom_alt['PRODUCTION_SITE'].unique())).rename(columns={0:'PRODUCTION_SITE'})
finished_goods = pd.DataFrame(list(proxy_bom['FG_CODE'].unique())).rename(columns={0:'FG_CODE'}).drop_duplicates()
finished_goods = finished_goods.merge(proxy_bom[['FG_CODE','FINISHED_GOOD']].drop_duplicates(), how = 'inner', on = 'FG_CODE')
production_sites = pd.concat([production_sites, plants.rename(columns={'PLANT_CODE':'PRODUCTION_SITE'})]).drop_duplicates()

print('\nEstratégia 02 :: Avaliando balanceamento alternativo de SKUs')
# 2025-07-18 :: Quero pegar todos os FG_CODE para todas as unidades
#bom_alt = bom_alt[(bom_alt['STATUS'] == 'Desbalanceada')].copy()
# Guardar um report com todas as fórmulas desbalanceadas
bom_alt = plants.merge(production_sites, how = 'cross')
bom_alt = bom_alt.merge(finished_goods, how = 'cross')
bom_alt = bom_alt.sort_values(by=['PLANT_CODE','PRODUCTION_SITE','FG_CODE'], ascending = True)
bom_alt = bom_alt[['PLANT_CODE','PRODUCTION_SITE','FG_CODE','FINISHED_GOOD']]
bom_alt = bom_alt.drop_duplicates().reset_index().drop(columns='index')
print('Obtendo lista técnica alternativa...')
bom_alt = fx.left_outer_join(bom_alt, proxy_bom, left_on = 'FG_CODE', right_on = 'FG_CODE', 
                             name_left = "Mesclagem (3) => BOM Alternativo", name_right="BOM-RESUMO", struct=False)
print('Eliminando valores vazios após a mesclagem...')
bom_alt = bom_alt.dropna(subset='FORMULA_CODE').reset_index().drop(columns='index')
bom_alt = bom_alt.sort_values(by=['PLANT_CODE','PRODUCTION_SITE','FG_CODE','RM_CODE'], ascending=True)
fg_list = []
lt = False
fechamento = 0.0

# ========================================= DEPRECADO ==================================================
# Não é necessário mais avaliar o fechamento, pois a partir da proxy_bom, todos fecham em 1.0
# bom_alt['ACC_QTY'] = 0.0
# for i in tqdm(range(bom_alt.shape[0]), desc = 'Avaliação Fechamento BOM (Alternativa)'):
#     if i == 0:
#         bom_alt['ACC_QTY'][i] = bom_alt['COMPONENT_QTY'][i]
#         fechamento = bom_alt['ACC_QTY'][i]
#         fg_list.append(bom_alt['FG_CODE'][i])
#     elif fechamento + bom_alt['COMPONENT_QTY'][i] <= 1.0 and bom_alt['FG_CODE'][i] in fg_list and lt == False:
#         bom_alt['ACC_QTY'][i] = fechamento + bom_alt['COMPONENT_QTY'][i]
#         fechamento = bom_alt['ACC_QTY'][i]
#     elif (fechamento + bom_alt['COMPONENT_QTY'][i] > 1.0 or lt == True ) and bom_alt['FG_CODE'][i] in fg_list:
#         lt = True
#         bom_alt['ACC_QTY'][i] = 0.0
#         fechamento = bom_alt['ACC_QTY'][i]
#     else: 
#         fg_list.append(bom_alt['FG_CODE'][i])
#         bom_alt['ACC_QTY'][i] = bom_alt['COMPONENT_QTY'][i]
#         fechamento = bom_alt['ACC_QTY'][i]
#         lt = False 
# ========================================= DEPRECADO ==================================================

bom_alt_sum = bom_alt.copy()
bom_alt_sum['COMPONENT_QTY'] = bom_alt_sum['COMPONENT_QTY'].apply(lambda x: round(x,3))
bom_alt_sum = bom_alt_sum.groupby(by=['PLANT_CODE','PRODUCTION_SITE','FG_CODE'])['COMPONENT_QTY'].sum().reset_index()
bom_alt_sum['COMPONENT_QTY'] = bom_alt_sum['COMPONENT_QTY'].round(3)
bom_alt_sum = bom_alt_sum[(bom_alt_sum['COMPONENT_QTY'] == 1)]
bom_alt_sum['CHECK']= 'X'
bom_alt_sum['ID'] = bom_alt_sum['PLANT_CODE'] + '-' + bom_alt_sum['PRODUCTION_SITE'] + '-' + bom_alt_sum['FG_CODE']
bom_alt_sum = bom_alt_sum[['ID','CHECK']]
bom_alt['ID'] = bom_alt['PLANT_CODE'] + '-' + bom_alt['PRODUCTION_SITE'] + '-' + bom_alt['FG_CODE']
bom_alt = fx.left_outer_join(bom_alt, bom_alt_sum, left_on='ID',right_on='ID',
                             name_left="Mesclagem (4) => BOM Alternativo", name_right="BOM-ALT-GRP")
bom_alt = bom_alt[(bom_alt['CHECK'] == 'X')].drop(columns='CHECK')
bom_alt = bom_alt[(bom_alt.ACC_QTY > 0.0)]
bom_alt = bom_alt.sort_values(by=['FG_CODE','FORMULA_CODE','ACC_QTY']).drop(columns='ACC_QTY')
bom_alt['TOTAL_COMP'] = 1.0
bom_alt['STATUS'] = 'Balanceada BASE-PF'
columns = ['PLANT_CODE','PRODUCTION_SITE','FG_CODE','FINISHED_GOOD','RM_CODE','RM_DESCRIPTION','COMPONENT_QTY','FORMULA_CODE','STATUS']
bom = bom[columns]
bom_alt = bom_alt.rename(columns={'FINISHED_GOOD_y':'FINISHED_GOOD'})[columns]
bom["PREFERENCIA"] = True
bom["check_preferencia"] = bom["PLANT_CODE"] + "-" + bom["PRODUCTION_SITE"] + "-" + bom["FG_CODE"]
bom_alt["check_preferencia"] = bom_alt["PLANT_CODE"] + "-" + bom_alt["PRODUCTION_SITE"] + "-" + bom_alt["FG_CODE"]
bom_alt = fx.left_outer_join(bom_alt, bom[["check_preferencia","PREFERENCIA"]].drop_duplicates(),
                             left_on="check_preferencia", right_on="check_preferencia",
                             name_left="Mesclage (5) => BOM ALT", name_right="Check Preferência")
bom_alt["PREFERENCIA"] = bom_alt["PREFERENCIA"].replace(True,False)
bom_alt["PREFERENCIA"] = bom_alt["PREFERENCIA"].fillna(True)
bom_alt = bom_alt[bom_alt["PREFERENCIA"] == True]
bom = pd.concat([bom, bom_alt])
bom = bom.sort_values(by=['PLANT_CODE','PRODUCTION_SITE','FG_CODE','RM_CODE'], ascending=True)
bom = bom.reset_index().drop(columns='index')
bom = fx.left_outer_join(bom, agrupamento_produtos_pf, left_on = 'FG_CODE', right_on = 'COD_ESPECIFICO',
                         name_left = "Mesclagem (6) => BOM", name_right="Agrupamento PF")
bom = fx.left_outer_join(bom, cadastro_pf, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM',
                         name_left = "Mesclagem (7) => BOM", name_right = "CADASTRO PF")
columns = ['PLANT_CODE', 'PRODUCTION_SITE', 'PRD-VCM','FG_CODE', 'FINISHED_GOOD', 
           'RM_CODE','RM_DESCRIPTION', 'COMPONENT_QTY', 'FORMULA_CODE', 'STATUS']
bom = bom[columns].rename(columns={'PRD-VCM':'PF-VCM'})
bom = fx.left_outer_join(bom,agrupamento_produtos_mp,left_on = 'RM_CODE', right_on = 'COD_ESPECIFICO',
                         name_left="Mesclagem (8) => BOM", name_right="Agrupamento MP")
bom = fx.left_outer_join(bom, cadastro_mp, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM',
                         name_left="Mesclagem (9) => BOM", name_right="Cadastro MP")
columns = ['PLANT_CODE', 'PRODUCTION_SITE', 'PF-VCM','FG_CODE', 'FINISHED_GOOD', 
           'PRD-VCM','RM_CODE','RM_DESCRIPTION', 'COMPONENT_QTY', 'FORMULA_CODE', 'STATUS']

bom = bom[columns].rename(columns={'PRD-VCM':'MP-VCM'})
bom = fx.left_outer_join(bom, unidades_produtoras, left_on = ['PLANT_CODE','PRODUCTION_SITE'], right_on = ['PLANTA','DEPOSITO'],
                         name_left="Mesclagem (10) => BOM", name_right="Unidades Produtoras")
columns = ['UNIDADE_VCM','PLANT_CODE', 'PRODUCTION_SITE', 'DESCRICAO_PLANTA', 'DESCRICAO_DEPOSITO', 'PF-VCM', 'FG_CODE',
           'FINISHED_GOOD', 'MP-VCM', 'RM_CODE', 'RM_DESCRIPTION', 'COMPONENT_QTY', 'FORMULA_CODE', 'STATUS']
bom = bom[columns]
bom = bom.sort_values(by=['PLANT_CODE','FG_CODE','FORMULA_CODE'])
# Lista Técnica conciliada para o VCM
demanda = fx.left_outer_join(demanda, agrupamento_produtos_pf, left_on='CODIGO PRODUTO', right_on='COD_ESPECIFICO',
                             name_left="Mesclagem (11) => Demanda", name_right="Agrupamento Produtos PF")
demanda = fx.left_outer_join(demanda, cadastro_pf, left_on='CODIGO_AGRUPADO', right_on='CODIGO_ITEM',
                             name_left="Mesclagem (12) => Demanda", name_right="Cadastro PF")
demanda['pk_sku_check'] = demanda['UNIDADE PRODUTORA'] + '-' + demanda['CODIGO PRODUTO']
bom['pk_sku_check'] = bom['PLANT_CODE'] + '-' + bom['FG_CODE']
demanda_grouped = demanda.groupby(by='pk_sku_check')['QUANTIDADE'].sum().reset_index()
bom = fx.left_outer_join(bom, demanda_grouped, left_on='pk_sku_check', right_on='pk_sku_check', 
                         name_left = 'Mesclagem (13) => BOM', name_right = 'DEMANDA AGRUPADA POR SKU')
bom['QUANTIDADE'] = bom['QUANTIDADE'].fillna(0.0)
bom = bom.rename(columns={'QUANTIDADE':'QUANTIDADE_NA_DEMANDA'})
demanda = fx.left_outer_join(demanda, bom[['pk_sku_check', 'STATUS']].drop_duplicates(subset='pk_sku_check'), 
                             left_on='pk_sku_check', right_on='pk_sku_check',
                             name_left="Mesclagem (X/19) => DEMANDA", name_right="BOM-PK")
bom = bom.drop(columns='pk_sku_check')
demanda['STATUS'] = demanda['STATUS'].fillna('Ausente')

try:
    os.makedirs(os.path.join(cwd, exec_log_path + "/[CALL TO ACTION] Lista Técnica Completa vs. Demanda Total/"))
except:
    pass

proxy_full = pd.DataFrame()
for files in bom["DESCRICAO_PLANTA"].unique():
    proxy = bom[bom["DESCRICAO_PLANTA"] == files].copy()
    proxy.to_excel(os.path.join(cwd, exec_log_path + "/[CALL TO ACTION] Lista Técnica Completa vs. Demanda Total/" + f"{str(files)}" + ".xlsx"), index=False)
    proxy_full = pd.concat([proxy_full, proxy[proxy['QUANTIDADE_NA_DEMANDA'] > 0.0]])

proxy_full.to_excel(os.path.join(cwd,exec_log_path + '[CALL TO ACTION] Fechamento Lista Técnica Completa vs. Demanda Total.xlsx'), index=False)

# Criar um log de erros da demanda para excel incluindo a unidade produtiva VCM
# ------------------------------------------------------------------------------

unique = unidades_terc['UNIDADE PRODUTORA'].drop_duplicates().to_list()
# Preencher proxy.Faturamento somente se for uma unidade terceira
demanda['proxy.Faturamento'] = demanda['UNIDADE PRODUTORA'].apply(lambda x: x if x not in unique else np.nan)
# Separar o arquivo de demanda com base na existência ou não na lista "unique"
demanda_unidade_standard = demanda[(demanda['proxy.Faturamento'].notna())].reset_index().drop(columns='index')
demanda_unidade_terceira = demanda[(demanda['proxy.Faturamento'].isna())].reset_index().drop(columns='index')
print(f"Foram identificados {demanda.loc[demanda['proxy.Faturamento'].isna(),:].shape[0]} linhas da demanda com problemas no depUnidadesGerencias.xlsx")
print('Verificar se todas as gerências estão cadastradas!')
# Para o DataFrame >> demanda_unidade_standard << a Unidade Faturamento é a própria unidade produtora
demanda_unidade_standard['UNIDADE FATURAMENTO'] = demanda_unidade_standard['proxy.Faturamento']
# Para o DataFrame >> demanda_unidade_terceira << a Unidade Faturamento é determinada através de regras
# 01: Se e existe uma CONSULTORIA específica no arquivo depUnidadesGerencias.xlsx, então, existe uma regra especial para 
# atribuir a UNIDADE FATURAMENTO a partir do depUnidadesGerencias, logo: >> demanda_unidade_terceira_na <<
# 02: Não existe CONSULTORIA específica, portanto, podemos seguir uma regra geral por GERENCIA, logo: >> demanda_unidade_terceira_notna <<
unique = unidades_terc.loc[unidades_terc['CONSULTORIA'].notna(),:]['GERENCIA'].drop_duplicates().to_list()
demanda_unidade_terceira['proxy.Consultoria'] = demanda_unidade_terceira['GERENCIA'].apply(lambda x: np.nan if x not in unique else x)
# Tratando os casos em que a CONSULTORIA está vazia
demanda_unidade_terceira_na = demanda_unidade_terceira.loc[demanda_unidade_terceira['proxy.Consultoria'].isna(),:].reset_index().drop(columns='index')
proxy_unidades_terc = unidades_terc.loc[unidades_terc['CONSULTORIA'].isna(),:].reset_index().drop(columns='index')
proxy_unidades_terc = proxy_unidades_terc.rename(columns={'UNIDADE PRODUTORA':'UNIDADE PRODUTORA.2',
                                                          'CONSULTORIA':'CONSULTORIA.2','GERENCIA':'GERENCIA.2'})
demanda_unidade_terceira_na = fx.left_outer_join(demanda_unidade_terceira_na, proxy_unidades_terc,
                              left_on = ['UNIDADE PRODUTORA','GERENCIA'],
                              right_on = ['UNIDADE PRODUTORA.2','GERENCIA.2'],
                              name_left='Unidades Terceiras', name_right='De-Para Unidades')
# Tratando as exceções em que a CONSULTORIA é relevante para determinação da UNIDADE DE FATURAMENTO
proxy_unidades_terc = unidades_terc.loc[unidades_terc['CONSULTORIA'].notna(),:].reset_index().drop(columns='index')
demanda_unidade_terceira_notna = demanda_unidade_terceira.loc[demanda_unidade_terceira['proxy.Consultoria'].notna(),:].reset_index().drop(columns='index')
proxy_unidades_terc = proxy_unidades_terc.rename(columns={'UNIDADE PRODUTORA':'UNIDADE PRODUTORA.2',
                                                          'CONSULTORIA':'CONSULTORIA.2','GERENCIA':'GERENCIA.2'})
demanda_unidade_terceira_notna = fx.left_outer_join(demanda_unidade_terceira_notna, proxy_unidades_terc,
                                 left_on = ['UNIDADE PRODUTORA','GERENCIA'],
                                 right_on = ['UNIDADE PRODUTORA.2','GERENCIA.2'],
                                 name_left='Unidades Terceiras', name_right='De-Para Unidades')
demanda = pd.concat([demanda_unidade_standard, demanda_unidade_terceira_na, demanda_unidade_terceira_notna])
demanda['UNIDADE PRODUTORA'] = demanda['UNIDADE PRODUTORA'].replace(list(dicgen['DE']), list(dicgen['PARA']))
demanda['UNIDADE FATURAMENTO'] = demanda['UNIDADE FATURAMENTO'].replace(list(dicgen['DE']), list(dicgen['PARA']))
demanda = fx.left_outer_join(demanda, unidades_produtoras[['DEPOSITO','PLANTA','UNIDADE_VCM']], left_on=['UNIDADE PRODUTORA','UNIDADE FATURAMENTO'],
                             right_on=['DEPOSITO','PLANTA'], name_left='DEMANDA', name_right='UNIDADES PRODUTORAS')
cols_to_drop = ['proxy.Faturamento','proxy.Consultoria','UNIDADE PRODUTORA.2','GERENCIA.2','CONSULTORIA.2','DEPOSITO','PLANTA']
demanda = demanda.drop(columns=cols_to_drop)
demanda.to_excel(os.path.join(cwd, exec_log_path + '[CALL TO ACTION] Demanda vs. Status Lista Técnica.xlsx'), index=False)
bom = bom.sort_values(by=['UNIDADE_VCM','FG_CODE','QUANTIDADE_NA_DEMANDA'])
bom['Proxy PR'] = bom['UNIDADE_VCM'] + '-' + bom['PF-VCM']
# bom_unique = bom.groupby(by=['UNIDADE_VCM','PF-VCM','MP-VCM','FORMULA_CODE','Proxy PR','PLANT_CODE','PRODUCTION_SITE']).agg(
#                         QUANTIDADE_NA_DEMANDA=('QUANTIDADE_NA_DEMANDA','max'),
#                         COMPONENT_QTY=('COMPONENT_QTY','sum')).reset_index()
# bom_unique = bom.groupby(by=['UNIDADE_VCM','PF-VCM','MP-VCM','FORMULA_CODE','Proxy PR','PLANT_CODE','PRODUCTION_SITE','COMPONENT_QTY']).agg(
#                         QUANTIDADE_NA_DEMANDA=('QUANTIDADE_NA_DEMANDA','max')).reset_index()
bom_unique = bom[['UNIDADE_VCM','PF-VCM','MP-VCM','FORMULA_CODE','Proxy PR','PLANT_CODE','PRODUCTION_SITE','COMPONENT_QTY','QUANTIDADE_NA_DEMANDA']]
bom_unique = bom_unique.sort_values(by=['FORMULA_CODE','PLANT_CODE','PRODUCTION_SITE','Proxy PR','MP-VCM'])
# bom_unique = bom.groupby(by=['UNIDADE_VCM','PF-VCM','MP-VCM','FORMULA_CODE','COMPONENT_QTY'])['QUANTIDADE_NA_DEMANDA'].max().reset_index()
# bom_unique = bom[['UNIDADE_VCM','PF-VCM','MP-VCM','FORMULA_CODE','COMPONENT_QTY','QUANTIDADE_NA_DEMANDA']]
bom_to_vcm = bom_unique.copy()

# ========================================= DEPRECADO ==================================================
# lt = False
# control_list = []
# formula_code = str()
# fechamento = 0.0
# bom_to_vcm['ACC_QTY'] = 0.0
# bom_to_vcm = bom_to_vcm.sort_values(by=['UNIDADE_VCM','PF-VCM','FORMULA_CODE']).reset_index().drop(columns='index')
# for i in tqdm(range(bom_to_vcm.shape[0]), desc = 'Avaliação Final :: Lista Técnica'):
#     if i == 0:
#         bom_to_vcm['ACC_QTY'][i] = bom_to_vcm['COMPONENT_QTY'][i]
#         fechamento = bom_to_vcm['ACC_QTY'][i]
#         control_list.append(bom_to_vcm['UNIDADE_VCM'][i] + '-' + bom_to_vcm['PF-VCM'][i])
#         formula_code = bom_to_vcm['FORMULA_CODE'][i]
#     elif fechamento + bom_to_vcm['COMPONENT_QTY'][i] <= 1.0 and formula_code == bom_to_vcm['FORMULA_CODE'][i]\
#     and bom_to_vcm['UNIDADE_VCM'][i] + '-' + bom_to_vcm['PF-VCM'][i] in control_list and lt == False:
#         bom_to_vcm['ACC_QTY'][i] = fechamento + bom_to_vcm['COMPONENT_QTY'][i]
#         fechamento = bom_to_vcm['ACC_QTY'][i]
#     elif (fechamento + bom_to_vcm['COMPONENT_QTY'][i] > 1.0 or lt == True or not formula_code == bom_to_vcm['FORMULA_CODE'][i]) \
#     and bom_to_vcm['UNIDADE_VCM'][i] + '-' + bom_to_vcm['PF-VCM'][i] in control_list:
#         lt = True
#         bom_to_vcm['ACC_QTY'][i] = 0.0
#         fechamento = bom_to_vcm['ACC_QTY'][i]
#     else: 
#         control_list.append(bom_to_vcm['UNIDADE_VCM'][i] + '-' + bom_to_vcm['PF-VCM'][i])
#         formula_code = bom_to_vcm['FORMULA_CODE'][i]
#         bom_to_vcm['ACC_QTY'][i] = bom_to_vcm['COMPONENT_QTY'][i]
#         fechamento = bom_to_vcm['ACC_QTY'][i]
#         lt = False
# bom_to_vcm = bom_to_vcm[(bom_to_vcm['ACC_QTY'] > 0.0)]


# Versão Vetorizada
# =================
# def process_bom(bom_to_vcm):
#     bom_to_vcm = bom_to_vcm.copy()
#     bom_to_vcm = bom_to_vcm.sort_values(by=['PLANT_CODE','PRODUCTION_SITE','Proxy PR','MP-VCM']).reset_index(drop=True)
#     bom_to_vcm['formula_key'] = bom_to_vcm['UNIDADE_VCM'] + '-' + bom_to_vcm['PF-VCM'] + '-' + bom_to_vcm['FORMULA_CODE'] + bom_to_vcm['PRODUCTION_SITE']
#     bom_to_vcm['cumulative_qty'] = bom_to_vcm.groupby('formula_key')['COMPONENT_QTY'].cumsum()
#     formula_totals = bom_to_vcm.groupby('formula_key')['COMPONENT_QTY'].sum().reset_index(name='total_qty')
#     bom_to_vcm = bom_to_vcm.merge(formula_totals, on='formula_key')
#     bom_to_vcm['ACC_QTY'] = 0.0
#     exact_formula_mask = np.isclose(bom_to_vcm['total_qty'], 1.0, atol=0.001)
#     bom_to_vcm.loc[exact_formula_mask, 'ACC_QTY'] = bom_to_vcm.loc[exact_formula_mask, 'COMPONENT_QTY']
#     non_exact_mask = ~exact_formula_mask
#     first_in_formula = ~bom_to_vcm.duplicated(['formula_key'])
#     below_threshold = bom_to_vcm['cumulative_qty'] <= 1.0
#     keep_mask = non_exact_mask & (first_in_formula | below_threshold)
#     bom_to_vcm.loc[keep_mask, 'ACC_QTY'] = bom_to_vcm.loc[keep_mask, 'COMPONENT_QTY']
#     exceeds_mask = non_exact_mask & (bom_to_vcm['total_qty'] > 1.0)
#     last_component_mask = (bom_to_vcm['cumulative_qty'] > 1.0) & (bom_to_vcm['cumulative_qty'].shift(1) <= 1.0)
#     adjustment = 1.0 - bom_to_vcm['cumulative_qty'].shift(1)
#     bom_to_vcm.loc[exceeds_mask & last_component_mask, 'ACC_QTY'] = adjustment
#     result = bom_to_vcm[bom_to_vcm['ACC_QTY'] > 0].copy()
#     return result.drop(columns=['formula_key', 'cumulative_qty', 'total_qty'])
# ========================================= DEPRECADO ==================================================

def process_bom(bom_to_vcm):
    bom_to_vcm = bom_to_vcm.copy()
    bom_to_vcm = bom_to_vcm.sort_values(by=['PLANT_CODE','PRODUCTION_SITE','Proxy PR','MP-VCM']).reset_index(drop=True)
    
    # Criando uma chave primária
    bom_to_vcm['formula_key'] = bom_to_vcm['UNIDADE_VCM'] + '-' + bom_to_vcm['PF-VCM'] + '-' + bom_to_vcm['FORMULA_CODE'] + '-' + bom_to_vcm['PRODUCTION_SITE']
    
    # Calculo de fechamento cumulativo
    bom_to_vcm['cumulative_qty'] = bom_to_vcm.groupby('formula_key')['COMPONENT_QTY'].cumsum()
    formula_totals = bom_to_vcm.groupby('formula_key')['COMPONENT_QTY'].sum().reset_index(name='total_qty')
    bom_to_vcm = bom_to_vcm.merge(formula_totals, on='formula_key')
    
    bom_to_vcm['ACC_QTY'] = 0.0
    
    # Caso 1: Fórmulas Exatas (total_qty ≈ 1.0)
    exact_formula_mask = np.isclose(bom_to_vcm['total_qty'], 1.0, atol=0.001)
    bom_to_vcm.loc[exact_formula_mask, 'ACC_QTY'] = bom_to_vcm.loc[exact_formula_mask, 'COMPONENT_QTY']
    
    # Caso 2: Formulas com total_qty < 1.0 - não considerar nenhum componentes
    below_total_mask = (bom_to_vcm['total_qty'] < 0.999) 
    bom_to_vcm.loc[below_total_mask, 'ACC_QTY'] = 0.0
    
    # Caso 3: Formulas com total_qty > 1.0 - pegar os componentes até fechar em 1.0
    exceeds_mask = (bom_to_vcm['total_qty'] > 1.001) & ~exact_formula_mask
    condition1 = exceeds_mask & (bom_to_vcm['cumulative_qty'] <= 1.0)
    bom_to_vcm.loc[condition1, 'ACC_QTY'] = bom_to_vcm.loc[condition1, 'COMPONENT_QTY']
    
    def adjust_exceeding_group(group):
        if group['total_qty'].iloc[0] <= 1.001:
            return group
        
        exceed_mask = group['cumulative_qty'] > 1.0
        if exceed_mask.any():
            first_exceed_idx = group[exceed_mask].index[0]
            prev_cumulative = group.loc[first_exceed_idx - 1, 'cumulative_qty'] if first_exceed_idx > group.index[0] else 0.0
            adjustment = 1.0 - prev_cumulative
            group.loc[first_exceed_idx, 'ACC_QTY'] = adjustment
            
            # Set all subsequent components to 0
            subsequent_mask = group.index > first_exceed_idx
            group.loc[subsequent_mask, 'ACC_QTY'] = 0.0
        
        return group
    
    bom_to_vcm = bom_to_vcm.groupby('formula_key').apply(adjust_exceeding_group).reset_index(drop=True)
    
    result = bom_to_vcm[bom_to_vcm['ACC_QTY'] > 0].copy()
    return result.drop(columns=['formula_key', 'cumulative_qty', 'total_qty'])

bom_to_vcm_processed = process_bom(bom_to_vcm)
bom_to_vcm = bom_to_vcm_processed.copy()
bom_to_vcm['Proxy PR'] = bom_to_vcm['UNIDADE_VCM'] + '-' + bom_to_vcm['PF-VCM']
bom_to_vcm = bom_to_vcm.dropna(subset=['UNIDADE_VCM','PF-VCM','MP-VCM'])
bom_to_vcm = bom_to_vcm.groupby(by=['UNIDADE_VCM','PF-VCM','MP-VCM','FORMULA_CODE','Proxy PR','PLANT_CODE','PRODUCTION_SITE'])['COMPONENT_QTY'].sum().reset_index()
bom_to_vcm = bom_to_vcm.sort_values(by=['UNIDADE_VCM','PF-VCM','Proxy PR' ,'FORMULA_CODE','PLANT_CODE','PRODUCTION_SITE']).reset_index().drop(columns='index')
# bom_to_vcm = bom_to_vcm.drop_duplicates(subset='Proxy PR', keep='first')
bom_to_vcm['pk'] = bom_to_vcm['Proxy PR'] + '-' + bom_to_vcm['FORMULA_CODE']
bom_to_vcm_sum = bom_to_vcm.copy()
bom_to_vcm_sum['COMPONENT_QTY'] = bom_to_vcm_sum['COMPONENT_QTY'].apply(lambda x: round(x,3))
bom_to_vcm_sum = bom_to_vcm_sum.groupby(by=['pk','Proxy PR'])['COMPONENT_QTY'].sum().reset_index()
bom_to_vcm_sum = bom_to_vcm_sum[(bom_to_vcm_sum['COMPONENT_QTY'] == 1.0)]
# A medida com base no DataFrame bom_to_vcm_sum não foi utilizada, mas fica disponível caso seja necessária
bom_to_vcm_sum = bom_to_vcm_sum.reset_index().drop(columns=['index','Proxy PR','COMPONENT_QTY'])
bom_to_vcm = fx.left_outer_join(df_left=bom_to_vcm, df_right=bom_to_vcm_sum, left_on='pk', right_on='pk')
bom_to_vcm = bom_to_vcm.sort_values(by=['pk','UNIDADE_VCM','PF-VCM','MP-VCM','PLANT_CODE','PRODUCTION_SITE'])
bom_to_vcm = bom_to_vcm.drop_duplicates(subset=['Proxy PR','MP-VCM'])
bom_to_vcm.to_excel(os.path.join(cwd,exec_log_path+'[CONSULTA] BOM PARA VCM.xlsx'))
bom_to_vcm = fx.left_outer_join(bom_to_vcm, template_saida, left_on = 'Proxy PR', right_on = 'Proxy PR',
                                name_left="Mesclagem (15) => BOM_TO_VCM", name_right="Template Saida")
template_entrada = fx.left_outer_join(template_entrada, bom_to_vcm, left_on = ['Unidade','Receita','Produto'],
                                      right_on = ['UNIDADE_VCM','Receita','MP-VCM'],
                                      name_left="Mesclagem (16) => Template Entrada", name_right="BOM_TO_VCM")
template_entrada = template_entrada[['Proxy PR','PF-VCM','Unidade', 'Receita', 'Produto', 'ValorEntrada',
                                     'MP-VCM', 'FORMULA_CODE', 'COMPONENT_QTY']]
template_saida['PF-VCM'] = template_saida['Proxy PR'].str.split('-PF',expand=True)[1]
template_saida['PF-VCM'] = 'PF' + template_saida['PF-VCM']
template_saida['Proxy PR'] = template_saida['Proxy PR'].str.split('-PF',expand=True)[0] + '-' + template_saida['Receita']
template_saida = template_saida.drop(columns='Receita')
template_saida = template_saida.rename(columns={'PF-VCM':'PF-VCM-SAIDA'})
template_entrada['Proxy PR'] = template_entrada['Unidade'] + '-' + template_entrada['Receita']
template_entrada = template_entrada.drop(columns='PF-VCM')
template_entrada = fx.left_outer_join(template_entrada, template_saida, left_on='Proxy PR', right_on='Proxy PR')
bom_to_vcm = bom_to_vcm.sort_values(by=['pk','MP-VCM','COMPONENT_QTY'], ascending = False)
proxy_bom = bom_to_vcm[['pk','PF-VCM','MP-VCM','COMPONENT_QTY']].copy().reset_index().drop(columns='index')
# proxy_bom['COMPONENT_QTY'] = np.trunc(1000*proxy_bom['COMPONENT_QTY'])/1000
proxy_bom['COMPONENT_QTY'] = proxy_bom['COMPONENT_QTY'].apply(lambda x: round(x,3))
proxy_bom = proxy_bom.sort_values(by='pk', ascending=False)
# proxy_bom = proxy_bom.reset_index().drop(columns=['pk','index'])
proxy_bom = proxy_bom.reset_index().drop(columns=['index'])
fg_list = []
lt = False
fechamento = 0.0
proxy_bom['ACC_QTY'] = 0.0

for i in tqdm(range(proxy_bom.shape[0]), desc='Avaliação de fechamento da BOM'):
    current_qty = proxy_bom['COMPONENT_QTY'].iloc[i]
    current_fg = proxy_bom['PF-VCM'].iloc[i]
    sum_check = fechamento + current_qty
    
    if i == 0:
        proxy_bom.at[i, 'ACC_QTY'] = current_qty
        fechamento = current_qty
        fg_list.append(current_fg)
    
    elif current_fg not in fg_list:
        fg_list.append(current_fg)
        proxy_bom.at[i, 'ACC_QTY'] = current_qty
        fechamento = current_qty
        lt = False
    
    elif lt:
        proxy_bom.at[i, 'ACC_QTY'] = 0.0
    
    elif sum_check <= 1.0:
        proxy_bom.at[i, 'ACC_QTY'] = sum_check
        fechamento = sum_check
    
    else:
        needed = truncate(1.0 - fechamento, 3)
        proxy_bom.at[i, 'ACC_QTY'] = needed
        fechamento = 1.0
        lt = True
        
        remaining = truncate(current_qty - needed, 3)
        if remaining > 0:
            pass
        
proxy_bom = proxy_bom[(proxy_bom['ACC_QTY'] != 0.0)].copy().reset_index().drop(columns='index')
proxy_bom = proxy_bom.rename(columns={'COMPONENT_QTY':'COMPONENT-QTY-NV2','MP-VCM':'PROXY-MP'})
proxy_bom = proxy_bom.drop_duplicates(subset=['PF-VCM','PROXY-MP'])
template_entrada = fx.left_outer_join(template_entrada, proxy_bom, left_on = ['PF-VCM-SAIDA','Produto'], right_on=['PF-VCM','PROXY-MP'])
template_entrada_sum = template_entrada.copy()
template_entrada_sum = template_entrada_sum.groupby(by=['Unidade','Receita'])['COMPONENT_QTY'].sum().reset_index()
template_entrada_sum['COMPONENT_QTY'] = template_entrada_sum['COMPONENT_QTY'].apply(lambda x: round(x,3))
template_entrada_sum = template_entrada_sum.rename(columns={'COMPONENT_QTY':'FECHAMENTO_NV1'})
proxy_bom_sum_nv2 = proxy_bom.groupby(by='PF-VCM')['COMPONENT-QTY-NV2'].sum().reset_index()
proxy_bom_sum_nv2['COMPONENT-QTY-NV2'] = proxy_bom_sum_nv2['COMPONENT-QTY-NV2'].apply(lambda x: round(x, 3))
proxy_bom_sum_nv2 = proxy_bom_sum_nv2.rename(columns={'COMPONENT-QTY-NV2': 'FECHAMENTO_NV2'})
template_entrada_sum = template_entrada_sum[(template_entrada_sum['FECHAMENTO_NV1'] == 1.0)]
template_entrada = fx.left_outer_join(template_entrada, template_entrada_sum, left_on = ['Unidade','Receita'],
                                      right_on = ['Unidade','Receita'], name_left="Mesclagem (16) => Template Entrada",
                                      name_right="Soma Template Entrada")
template_entrada = fx.left_outer_join(template_entrada, proxy_bom_sum_nv2, left_on='PF-VCM', right_on='PF-VCM')
template_entrada['COMPONENT_QTY'] = template_entrada['COMPONENT_QTY'].fillna(0.0)
template_entrada['COMPONENT-QTY-NV2'] = template_entrada['COMPONENT-QTY-NV2'].fillna(0.0)
# condicao = [(template_entrada['FECHAMENTO_NV1'] == 1.0) & (template_entrada['MP-VCM'] == template_entrada['Produto']),
condicao = [(template_entrada['FECHAMENTO_NV1'] == 1.0),
            (template_entrada['FECHAMENTO_NV2'] == 1.0) \
            & (template_entrada['PROXY-MP'] == template_entrada['Produto'])]

resultado = [template_entrada['COMPONENT_QTY'], template_entrada['COMPONENT-QTY-NV2']]

template_entrada['ValorEntrada'] = np.select(condicao, resultado, default=0.0)

# ================================================= DEPRECADO ============================================================
# VERSÃO VETORIZADA DO PREENCHIMENTO DE DADOS
# JUSTIFICATIVA: a diferença de processamento não justifica abrir mão do elemento visual para o usuário
# for i in tqdm(range(template_entrada.shape[0]), desc = 'Preenchimento do Template de Entrada'):
#     if template_entrada['FECHAMENTO'][i] == 1.0 and template_entrada['MP-VCM'][i] == template_entrada['Produto'][i]:
#         template_entrada['ValorEntrada'][i] = template_entrada['COMPONENT_QTY'][i]
#     elif template_entrada['FECHAMENTO'][i] < 1.0 and template_entrada['COMPONENT_QTY_NV2'][i] > 0.0:
#         template_entrada['ValorEntrada'][i] = template_entrada['COMPONENT_QTY_NV2'][i]
#     else:
#         template_entrada['ValorEntrada'][i] = 0.0
# ================================================= DEPRECADO ============================================================

template_entrada = template_entrada[['Unidade','Receita','Produto','ValorEntrada']]
template_entrada['ValorEntrada'] = template_entrada['ValorEntrada'].apply(lambda x: round(x,3))

# ================================================= DEPRECADO ============================================================
# template_entrada.to_excel(os.path.join(cwd,output_path + 'tbOutRendimentosEntrada.xlsx'),
#                           index = False, sheet_name = 'RENDIMENTO_ENTRADA_PROD')
# ================================================= DEPRECADO ============================================================

template_entrada.to_csv(os.path.join(cwd, output_path + 'tbOutRendimentoEnt.csv'),
                        index = False, sep = ';', encoding = 'utf-8-sig')
template_entrada_sum = template_entrada.groupby(by=['Unidade','Receita'])['ValorEntrada'].sum().reset_index()
template_saida_corr = fx.left_outer_join(df_left=template_saida_corr, df_right=template_entrada_sum, left_on=['Unidade','Receita'], right_on=['Unidade','Receita'])
template_saida_corr.to_excel(os.path.join(cwd,exec_log_path + '[CALL TO ACTION] Lista de Rendimentos de Saída Zerados.xlsx'),
                             index = False, sheet_name = 'RendSaidaExpurgado')
template_saida_corr['ValorSaida'] = np.where((template_saida_corr['ValorSaida']==1.0)&(template_saida_corr['ValorEntrada'] == 1.0),
                                             template_saida_corr['ValorSaida'], 0.0)
template_saida_corr.to_csv(os.path.join(cwd, output_path + 'tbOutRendimentoSai_corrigido.csv'),
                                    index=False, sep = ';', encoding='utf-8')

end_time = time.time()
print(f'\nTempo de Execução: {round(end_time - start_time,2)} segundos')