print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                             >>  warehouses.py  <<                                              ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 23/04/2025                                                ║')
print('║ Editado por:   Murilo Lima Ribeiro             Data: 25/08/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v2.0.0 (25/08/2025): Release Projeto Merger                                                                  ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Este script é responsável pela atualização:                                                                    ║')
print('║ >> Custos de Armazenagem e Handling                                                                            ║')
print('║ >> Capacidades de Armazenagem                                                                                  ║')
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

print('Carregando arquivos necessários... \n')
print('Tempo de execução esperado: por volta de 4 min \n')

# DataFrame ::  Dicionário Genérico
dicgen = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['dicgen']),
                       sheet_name = arquivos_primarios['dicgen'].split('.')[0],
                       usecols = list(tp_dado_arquivos['dicgen'].keys()),
                       dtype = tp_dado_arquivos['dicgen'])

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

# DataFrame :: DEPARA UNIDADES DE ARMAZENAGEM
df_unidades_armz = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['localizacao']),
                         sheet_name= arquivos_primarios['localizacao_sn'],
                         usecols=list(tp_dado_arquivos['localizacao'].keys()),
                         dtype=tp_dado_arquivos['localizacao'])

# DataFrame :: DADO PRIMARIO DE ARMAZENAGEM E HANDLING
df_custos_armz = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['custos_armz']),
                         sheet_name= arquivos_primarios['custos_armz_sn'],
                         usecols=list(tp_dado_arquivos['custos_armz'].keys()),
                         dtype=tp_dado_arquivos['custos_armz']).applymap(fx.padronizar)

# DataFrame :: Dado Primário de Custo Financeiro
df_custo_financeiro = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['custos_armz']),
                         sheet_name= arquivos_primarios['df_custo'],
                         usecols=list(tp_dado_arquivos['df_custo'].keys()),
                         dtype=tp_dado_arquivos['df_custo'])

# DataFrame :: DADO PRIMARIO DE CAPACIDADE DE ARMAZENAGEM INTERNA E EXTERNA
df_cap_armz = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cap_prod']),
                         sheet_name= arquivos_primarios['cap_prod_sn'],
                         usecols=list(tp_dado_arquivos['cap_prod'].keys()),
                         dtype=tp_dado_arquivos['cap_prod']).applymap(fx.padronizar)

df_cap_arm_maxmin = df_cap_armz.copy()

# DataFrame :: Unidades de Expedição e Descarga
# DataFrame :: Depara Unidades Produtivas / Armazenagem / Expedição
df_unidades = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_exp']),
                         sheet_name= arquivos_primarios['unidades_exp_sn'], 
                       usecols=list(tp_dado_arquivos['unidades_exp'].keys()),
                       dtype=tp_dado_arquivos['unidades_exp']).applymap(fx.padronizar)
unid_arm = df_unidades[['DEPOSITO','PLANTA','UNIDADE_ARMAZENAGEM_VCM']]

# DataFrame :: Horizonte (Período) de Otimização
df_periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']),
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])
df_periodos = df_periodos.rename(columns=rename_dataframes['df_periodos'])

# Dataframe :: Custo de Reposição
custos_mp = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['custo_reposicao']),
                                  sheet_name = arquivos_primarios['custo_reposicao_sn'],
                                  usecols = list(tp_dado_arquivos['custo_reposicao'].keys()),
                                  dtype = tp_dado_arquivos['custo_reposicao'])
custos_mp = custos_mp.loc[custos_mp['CUSTO_REPOSICAO_MERCADO'] > 0.0,:].reset_index().drop(columns='index')

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

# Dataframe :: Demurrage
demurrage = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['demurrage']),
                                  sheet_name = arquivos_primarios['demurrage_sn'],
                                  dtype = tp_dado_arquivos['demurrage'])
ptax_demurrage = pd.read_excel(os.path.join(path + arquivos_primarios['demurrage']), sheet_name = 'PTAX',
                                  dtype =tp_dado_arquivos['ptax'])

# Dataframe :: Template Suprimento
#fx.validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['wizard_suprimento_faixa']))
template_suprimento = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['wizard_suprimento_faixa']),
                                  usecols = list(tp_dado_arquivos['wizard_suprimento_faixa'].keys()),
                                  dtype = tp_dado_arquivos['wizard_suprimento_faixa'])
wizard_suprimento_faixa = template_suprimento[['Unidade', 'Produto', 'Periodo']]

# DataFrame :: TEMPLATE DE CUSTO DE HANDLING PARA ARMAZÉNS EXTERNOS
#validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_hand_armz']))
df_template_hand_armz = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_hand_armz']),
                         usecols=list(tp_dado_arquivos['template_hand_armz'].keys()),
                         dtype=tp_dado_arquivos['template_hand_armz'])
df_template_hand_armz['Recebimento'] = 0.0
df_template_hand_armz['Expedição'] = 0.0

# DataFrame :: TEMPLATE DE CUSTOS VARIAVEIS PARA ARMAZÉNS EXTERNOS
#validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_var_armz']))
df_template_var_armz = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_var_armz']),
                         usecols=list(tp_dado_arquivos['template_var_armz'].keys()),
                         dtype=tp_dado_arquivos['template_var_armz'])
df_template_var_armz['Valor'] = 0.0
df_template_var_armz['Custo Financeiro'] = 0.0
df_template_var_armz['Custo Variável'] = 0.0

# DataFrame :: Template Capacidade
#validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_capacidade']))
template_capacidade = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_capacidade']),
                       usecols=list(tp_dado_arquivos['template_capacidade'].keys()),
                       dtype=tp_dado_arquivos['template_capacidade'])
template_capacidade['Volume Mínimo'] = 0.0
template_capacidade['Volume Máximo'] = 0.0

# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================

print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║ >> Etapa 01/02: Preenchimento de Custos de Handling e Armazenagem <<                                           ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
# ==========================================
# >>>>>>>>> POR ESTADO TÁ ERRADO!! <<<<<<<<<
# ==========================================
# Tópico 1: (faremos por estado só por enquanto)
# Mesclando o template com o depGeolocalizacao pela unidade, para pegar o Estado.
# Faria sentido puxar isso por unidade direto? 
# (Sem passar pelo estado, mas ai ia ter que vir a unidade no dado de custo)

df_template_hand_armz = fx.left_outer_join(df_template_hand_armz,df_unidades_armz,left_on='Unidade', right_on='Unidade',
                                           name_left='Template de Custos de Handling', name_right='Localização')
df_template_hand_armz = fx.left_outer_join(df_template_hand_armz,df_custos_armz,left_on=['Estado','Unidade'], right_on=['Estado','Terceiro'],
                                           name_left='Template de Custos de Handling', name_right='DADO PRIMARIO DE ARMAZENAGEM E HANDLING')
df_template_hand_armz['Recebimento'] = df_template_hand_armz['Handling (R$/ton)'].fillna(0.0)
df_template_hand_armz = df_template_hand_armz[['Unidade','Produto','Periodo','Recebimento','Expedição']]
df_template_hand_armz.to_excel(os.path.join(cwd,output_path+'tbOutCustosHandlingArmz.xlsx'),index=False, sheet_name='HANDLING')

# print('╔════════════════════════════════════════════════════════════════════════════════════════╗')
# print('║ >> Preenchimento de Custos de Armazenagem <<                                           ║')
# print('╚════════════════════════════════════════════════════════════════════════════════════════╝')

df_template_var_armz = fx.left_outer_join(df_template_var_armz,df_unidades_armz,left_on='Unidade', right_on='Unidade',
                                          name_left='Template de Custos Variáveis', name_right='Localização')

# (08/08/2025) :: Linhas abaixo duplicadas do script reposition_cost.py

# CUSTOS DE FORNECIMENTO DE MATÉRIAS-PRIMAS
# =========================================

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
demurrage = demurrage.merge(df_periodos, how = 'left', left_on = 'Periodo', right_on = 'Nome')

demurrage = demurrage.merge(df_portos, how = 'left', left_on = 'Porto', right_on = 'PORTO')

demurrage['ID-RIGHT'] = demurrage['NOME_PORTO_VCM'] + '-' + demurrage['Nome VCM']

# (Essas coisas já estavam anotadas no script de FTO)
# Incluir uma coluna denominada DEMURRAGE USD - PREMIUM para replicar os dados de DEMURRAGE USD
# Renomear o nome dos portos para remover o nível de premium
# Criar um dataframe agrupado
demurrage['Demurrage BRL - PREMIUM'] = demurrage['Demurrage BRL']
demurrage = demurrage.groupby(['Porto','Terminal','Periodo','ID-RIGHT']).agg({'Demurrage BRL':'min','Demurrage BRL - PREMIUM':'max'})
demurrage = demurrage.reset_index()
custos_mp = custos_mp.rename(columns = {'DT_INICIAL':'Data Inicial','DT_FINAL':'Data Final'})
custos_mp = custos_mp.merge(df_periodos[['Nome','Nome VCM']], how = 'cross')

# Caso haja período do VCM sem valor, usa o LAST_UPDATED_COST
last_updated_cost = custos_mp.copy()
last_updated_cost = last_updated_cost[['Nome','CD_PRODUTO','CODIGO_MOEDA','CUSTO_REPOSICAO_MERCADO']]
last_updated_cost = last_updated_cost.sort_values(by = 'Nome', ascending = False)
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
last_updated_cost = last_updated_cost[['Custo VCM (BRL/ton)','PRD-VCM','CD_PRODUTO']]
last_updated_cost = last_updated_cost.dropna().reset_index().drop(columns = 'index')
last_updated_cost = last_updated_cost[['PRD-VCM','Custo VCM (BRL/ton)']].rename(columns = {'Custo VCM (BRL/ton)':'LAST_UPDATED_COST','PRD-VCM':'ID'})

# (11/02/2025) Olhando primeiro o código específico, depois o agrupado.
custos_mp = fx.left_outer_join(custos_mp, agrupamento_produtos, right_on = 'COD_ESPECIFICO', left_on = 'CD_PRODUTO',
                                                  name_left='Custo de Reposição', name_right='Agrupamento de Produtos')

# Criar regra para estabelecer períodos
# Utilizar LAST_UPDATED_COST caso False
custos_mp['Validar'] = (custos_mp['Nome'] >= custos_mp['Data Inicial']) & (custos_mp['Nome'] <= custos_mp['Data Final'])
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
custos_mp = custos_mp.groupby(by=['Nome VCM','PRD-VCM_x'])['Custo VCM (BRL/ton)'].mean().reset_index()
custos_mp = custos_mp.merge(last_updated_cost, how = 'left', left_on = 'PRD-VCM_x', right_on = 'ID')
custos_mp = custos_mp.rename(columns={'PRD-VCM_x':'PRD-VCM'})
# (12/02/2025) Fazendo uma média para o last_updated_cost, já que temos o mesmo MP,
# mas códigos diferentes, então temos custos diferentes para cada código.
custos_mp = custos_mp.groupby(by=['Nome VCM','PRD-VCM','Custo VCM (BRL/ton)'])['LAST_UPDATED_COST'].mean().reset_index()
wizard_custo_suprimento_faixa = template_suprimento.drop(columns = ['Suprimento Mínimo', 'Suprimento Máximo'])
wizard_custo_suprimento_faixa = df_template_var_armz.copy()
wizard_custo_suprimento_faixa['ID-LEFT'] = wizard_custo_suprimento_faixa['Unidade'] + '-' + wizard_custo_suprimento_faixa['Periodo']

# (04/07/2025) Retirando duplicatas por PRD-VCM, pois isso estava alterando a estrutura do template.
agrupamento_produtos = agrupamento_produtos.drop_duplicates(subset=['PRD-VCM'])

demurrage = fx.left_outer_join(demurrage, df_periodos, left_on = 'Periodo', right_on = 'Nome',
                               name_left='Demurrage', name_right='Períodos')
demurrage = fx.left_outer_join(demurrage, df_portos, left_on = 'Porto', right_on = 'PORTO',
                               name_left='Demurrage', name_right='Portos')
demurrage['ID-RIGHT'] = demurrage['NOME_PORTO_VCM'] + '-' + demurrage['Nome VCM']
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
        if  wizard_custo_suprimento_faixa['Produto'][i] == custos_mp['PRD-VCM'][j] and wizard_custo_suprimento_faixa['Periodo'][i] == custos_mp['Nome VCM'][j]:
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

# >> Encerra aqui a lógica replicada de reposition_cost.py <<

df_template_var_armz = df_template_var_armz.merge(wizard_custo_suprimento_faixa, how='left',
                                                                    on=['Unidade','Produto','Periodo'])
df_template_var_armz['Custo do Produto'] = df_template_var_armz['Custo do Produto'].fillna(0.0)
df_template_var_armz['Valor'] = df_template_var_armz['Custo do Produto'].round(2)
df_template_var_armz = df_template_var_armz.drop(columns={'Custo do Produto'})

df_template_var_armz = fx.left_outer_join(df_template_var_armz,df_custos_armz,left_on=['Estado','Unidade'], right_on=['Estado','Terceiro'],
                                          name_left='Template de Custos Variáveis', name_right='DADO PRIMARIO DE ARMAZENAGEM E HANDLING')
df_template_var_armz['Custo Variável'] = df_template_var_armz['Armazenagem (R$/ton)'].fillna(0.0)
# (03/12/2024) Como pedido pelo Ricardo, caso o custo variável esteja zerado, preencher com um valor específico para cada unidade.
df_template_var_armz['ID'] = df_template_var_armz['Unidade'].str[:3]
custo_paliativo = {'ID':['AEX', 'APO', 'AIN', 'TER'],\
                      'Custo Paliativo':[25, 50, 1, 35]}
custo_paliativo = pd.DataFrame.from_dict(custo_paliativo)
df_template_var_armz = df_template_var_armz.merge(custo_paliativo, how='left', on='ID')
df_template_var_armz['Custo Variável'] = df_template_var_armz.apply(lambda x: x['Custo Paliativo'] if 
                                                            x['Custo Variável']==0.0 else x['Custo Variável'], axis=1)
df_template_var_armz['Custo Financeiro'] = 0.0
df_template_var_armz['Custo Financeiro'] = np.where(df_template_var_armz['Custo Financeiro'] == 0.0,
                                                               df_custo_financeiro['Custo'], df_template_var_armz['Custo Financeiro'])
df_template_var_armz = df_template_var_armz[['Unidade','Produto','Periodo','Valor','Custo Financeiro','Custo Variável']]
df_template_var_armz.to_excel(os.path.join(cwd,output_path+'tbOutCustosVariaveisArmz.xlsx'),index=False, sheet_name='CUSTOS_VARIAVEIS')
print('\nFinalizado: Wizard de Custos de Armazenagem')

# (08/07/2025) Como conversado com o Matheus, trazendo de limits a etapa abaixo.
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║  >> Etapa 02/02:  LIMITES DE CAPACIDADE MÍNIMO E MÁXIMO  <<                                                    ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ # Popula a capacidade de armazenagem interno (AIN) e externo (AEX)                                             ║')
print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')

# (08/07/2025) Como conversado com o Matheus, retirando a coluna de 
# data de referência para armazenagem, para evitar problemas.
df_cap_arm_maxmin = df_cap_arm_maxmin.loc[df_cap_arm_maxmin['Agrupador']=='CAPACIDADE ARMAZENAGEM'].copy()
unid_arm['Local'] = np.where(unid_arm['UNIDADE_ARMAZENAGEM_VCM'].str[:3] == 'AIN','INTERNO','EXTERNO')
unid_arm = unid_arm.loc[unid_arm['DEPOSITO']=='1001']

# 2025-08-11 :: Deprecando depósito = 1001 por decisão do negócio, pois precisamos
# diferencias as capacidades internas e externas
df_cap_arm_interno = df_cap_arm_maxmin[df_cap_arm_maxmin['Local'] == 'INTERNO'].copy()
df_cap_arm_externo = df_cap_arm_maxmin[df_cap_arm_maxmin['Local'] == 'EXTERNO'].copy()

# CAPACIDADE :: INTERNO
df_cap_arm_interno['Unidade'] = df_cap_arm_interno['Unidade'].replace(list(dicgen['DE']),list(dicgen['PARA']))
df_cap_arm_interno = df_cap_arm_interno[['Unidade','Quantidade','Local']]
df_cap_arm_interno = fx.left_outer_join(df_cap_arm_interno, unid_arm, left_on=['Unidade','Local'], right_on = ['PLANTA','Local'],
                   name_left='Capacidade de Armazenagem INTERNO', name_right='Depara de Unidades')
# (08/07/2025) Adicionando o período como cross, pq cada linha precisa de uma referência de período para o template!
df_cap_arm_interno = df_cap_arm_interno.merge(df_periodos, how='cross')

# CAPACIDADE :: EXTERNO
# Obtendo o DE-PARA para as unidades externas sem especificar unidade faturamentora
df_unidades_limites = df_unidades[df_unidades['NOME_UNIDADE_LIMITES'].notna()].copy()
df_cap_arm_externo = fx.left_outer_join(df_left = df_cap_arm_externo, 
                                     df_right = df_unidades_limites[['NOME_UNIDADE_LIMITES','UNIDADE_ARMAZENAGEM_VCM']],
                                     left_on = 'Nome Unidade', right_on = 'NOME_UNIDADE_LIMITES',
                                     name_left = 'CAPACIDADE PRODUCAO', name_right = 'DEPARA DE UNIDADES',
                                     struct=False)
# (08/07/2025) Adicionando o período como cross, pq cada linha precisa de uma referência de período para o template!
df_cap_arm_externo = df_cap_arm_externo.merge(df_periodos, how='cross')
df_cap_arm_externo = df_cap_arm_externo.dropna(subset='UNIDADE_ARMAZENAGEM_VCM')
df_cap_arm_externo = df_cap_arm_externo.groupby(by=['UNIDADE_ARMAZENAGEM_VCM','Nome VCM'])['Quantidade'].mean().reset_index()

# Mesclar DataFrames
cols_cap = {
    'UNIDADE_ARMAZENAGEM_VCM':'UNIDADE_ARMAZENAGEM_VCM',
    'Nome VCM':'Nome VCM',
    'Quantidade':'Quantidade',
}

df_cap_arm_externo = df_cap_arm_externo[list(cols_cap.keys())]
df_cap_arm_interno = df_cap_arm_interno[list(cols_cap.keys())]
df_cap_arm_maxmin = pd.concat([df_cap_arm_interno, df_cap_arm_externo])
#df_cap_arm_maxmin = df_cap_arm_maxmin.rename(columns=cols_cap)

template_capacidade = fx.left_outer_join(template_capacidade, df_cap_arm_maxmin, left_on=['Unidade','Periodo'], right_on=['UNIDADE_ARMAZENAGEM_VCM','Nome VCM'],
                                         name_left = 'Template Capacidade', name_right = 'Capacidade de Armazenagem INTERNO e EXTERNO')
template_capacidade['Volume Máximo'] = template_capacidade['Quantidade']
template_capacidade = template_capacidade[['Unidade','Periodo','Volume Mínimo','Volume Máximo']]
template_capacidade = template_capacidade.rename(columns={'Unidade_x':'Unidade'})
template_capacidade['Volume Máximo'] =  template_capacidade['Volume Máximo'].fillna(500000)
template_capacidade['Volume Máximo'] =  template_capacidade['Volume Máximo'].round(2)
template_capacidade.to_excel(os.path.join(cwd,output_path+'tbOutCapacidadeArmazenagem.xlsx'), index=False, sheet_name='VOLUME_AGRUPADO')

# (08/07/2025) Como conversado com o Matheus, desativando o trecho abaixo,
# pois estamos usando a lógica que estava em limits.
# print('\n')
# print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
# print('║ Etapa 02/02: Preenchimento de Capacidades de Armazenagem                                                       ║')
# print('╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝')
# df_cap_armz = df_cap_armz.loc[df_cap_armz['Agrupador']=='CAPACIDADE ARMAZENAGEM']
# df_cap_armz = df_cap_armz.merge(df_unidades_armz, how = 'left', left_on = 'Unidade', right_on='Abrev-P02')
# left_outer_join(df_template_cap_amrz,df_cap_armz,left_on='Unidade', right_on='Unidade_y')
# df_template_cap_amrz['Vol. Max. Aj.'] = df_template_cap_amrz.apply(lambda x: x['Quantidade'] if x['Quantidade'] > 0.0 else x['Volume Máximo'], axis = 1)
# cols = ['Unidade','Periodo','Vol. Max. Aj.']
# df_template_cap_amrz = df_template_cap_amrz[cols].rename(columns={'Vol. Max. Aj.':'Limite'})
# # (06/11/2024) Regra para preeencher com 100.000 os Volumes Máximos que forem iguais a 0
# df_template_cap_amrz['Limite'] = df_template_cap_amrz.apply(lambda x: 100000 if x['Limite'] == 0.0 and x['Unidade'][:3]!='APO' else x['Limite'], axis = 1)
# df_template_cap_amrz = df_template_cap_amrz.loc[df_template_cap_amrz['Limite']!=0.0]
# df_template_cap_amrz.to_excel(os.path.join(cwd,output_path+'tbOutCapacidadeArmazenagem.xlsx'),
#                                       index = False, sheet_name = 'VOLUME_AGRUPADO')
print('\nFinalizado: Wizard de Capacidade de Armazenagem')
end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')