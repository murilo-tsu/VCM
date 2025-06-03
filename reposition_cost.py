print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                           >>  reposition_cost.py  <<                                           ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 28/04/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 30/04/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (30/04/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
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

# CHECAGEM DE ARQUIVOS
# >> Valida a data 
def validar_data_arquivo(arquivo):
    try:
        
        timestamp = os.path.getmtime(arquivo)
        # Obter data e hora do momento da atualização
        curr_date = time.localtime()
        comp_timestamp = time.localtime(timestamp)

        # Converter em um objeto do tipo datetime
        data_edicao = datetime.datetime.fromtimestamp(timestamp)        

        # Exibe a data em um pop-up
        if curr_date.tm_mon > comp_timestamp.tm_mon and curr_date.tm_year >= comp_timestamp.tm_year:
            messagebox.showinfo("Script Encerrado!!!", f'O arquivo {arquivo} está desatualizado.\nÚltima atualização em: {data_edicao}')
            sys.exit()
    
    except FileNotFoundError: 
        messagebox.showerror("Erro", "Arquivo não encontrado.")

def left_outer_join(df_left, df_right, left_on, right_on):
    print('\n')
    print(f'══════════════════════════════════════════════ LEFT JOIN ═══════════════════════════════════════════════════')
    name_left = [name for name, obj in globals().items() if obj is df_left]
    name_right = [name for name, obj in globals().items() if obj is df_right]
    print(f'Mesclando {name_left} x {name_right}')
    x1 = df_left.shape[0]
    print(f'A quantidade de linhas antes do join é {x1}')
    merged_df = df_left.merge(df_right, how = 'left', left_on = left_on, right_on = right_on)
    # Limpar o DataFrame original e aplicar as novas colunas
    df_left.drop(df_left.columns, axis=1, inplace=True) 
    for col in merged_df.columns:
        df_left[col] = merged_df[col]  # Copiar colunas do merged_df

    x2 = df_left.shape[0]
    print(f'A quantidade de linhas após o join é {x2}')
    if x1 == x2:
        y = '√'
    else:
        y = 'X'
        print(f'Checar por duplicidades em {name_right}')
    print(f'═══════════════════════════════════════ FIM DO JOIN :: Resultado = {y} ═══════════════════════════════════════')

# PADRONIZAR STRINGS
def padronizar(value):
    if isinstance(value, str):
        value = value.upper().strip()
        value = unidecode(value)
    return value


# =======================================================================================================================
# DEFINIR ARQUIVOS
# =======================================================================================================================

arquivos_primarios = {
     'periodos': 'iptPeriodos.xlsx',
     'periodos_sn': 'Períodos de Otimização',
     'portos': 'depUnidadesPortuarias.xlsx',
     'portos_sn': 'depUnidadesPortuarias',
     'cadastro_produtos': 'depSKU.xlsx',
     'cadastro_produtos_sn': 'CADASTRO',
     'agrupamento_sn':'AGRUPAMENTO',
     'compras_importadas': 'iptComprasImportadas.xlsx',
     'compras_importadas_sn': 'iptComprasImportadas',
     'compras_nacionais':'iptComprasNacionais.xlsx',
     'compras_nacionais_sn':'iptComprasNacionais',
     'custos_mp': 'iptCustoReposicao.xlsx',
     'custos_mp_sn': 'iptCustoReposicao',
     'demurrage': 'iptDemurrage.xlsx',
     'demurrage_sn': 'iptDemurrage',
     'ptax_demurrage': 'PTAX',
     'template_suprimento': 'tmpSuprimentoFaixa.xlsx',
}

tp_dado_arquivos = {
     'periodos':{'NUMERO':np.int64,'PERIODO':'datetime64[ns]', 'NOME_PERIODO':str},
     'portos': {'NOME_PORTO_VCM':str, 'PORTO':str, 'UNIDADE':str, 'CORRENTE':str},
     'cadastro_produtos': {'PRD-VCM':str,'CODIGO_ITEM':str,'DESCRICAO':str, 'TIPO_MATERIAL':str, 'CATEGORIA':str},
     'agrupamento': {'COD_ESPECIFICO':str, 'CODIGO_AGRUPADO':str},
     'compras_importadas': {'Porto':str,'Fábrica':str,'Matéria-prima':str,'Mês Entrega':'datetime64[ns]',
                   'BALANCE (TONS)':np.float32,'Status':str,'COMPANY':str,'RAW MATERIAL COD.':str},
     'compras_nacionais': {'Porto':str,'Fábrica':str,'Matéria-prima':str,'Status':str,'COMPANY':str,
                            'RAW MATERIAL COD.':str,'Mês000':np.float32,'Mês001':np.float32,'Mês002':np.float32,
                            'Mês003':np.float32,'Mês004':np.float32,'Mês005':np.float32,'Mês006':np.float32,
                            'Mês007':np.float32,'Mês008':np.float32,'Mês009':np.float32,'Mês010':np.float32,
                            'Mês011':np.float32,'Mês012':np.float32},
     'custos_mp': {'DH_VIGOR':'datetime64[ns]', 'DH_REFERENCIA':'datetime64[ns]', 'DT_INICIAL':'datetime64[ns]', 
                    'DT_FINAL':'datetime64[ns]', 'CD_PRODUTO_FTO':str, 'DESCRICAO_ITEM':str, 'CODIGO_ORGANIZACAO':str,
                    'CODIGO_MOEDA':str, 'PTAX_DIA_ANTERIOR':np.float64, 'CUSTO_REPOSICAO_MERCADO':np.float64},
     'demurrage': {'Porto':str, 'Terminal':str},
     'ptax': {'Cotação (BRL/USD)': np.float64},
     'template_suprimento': {'Unidade':str, 'Produto':str, 'Periodo':str, 'Suprimento Mínimo':np.float64, 'Suprimento Máximo':np.float64},
}

rename_dataframes = {
    'df_periodos':{'NOME_PERIODO':'Periodo_VCM', 'PERIODO':'Nome'},
    'df_revisao_importada':{'Porto':'PORTO','Fábrica':'PLANTA','Matéria-prima':'MP','Mês Entrega':'DT_REMESSA',
                    'BALANCE (TONS)':'BALANCE_TONS','Status':'STATUS','COMPANY':'COMPANY','RAW MATERIAL COD.':'CODIGO_MP'},
    'df_revisao_nacional':{'Porto':'PORTO','Fábrica':'PLANTA','Matéria-prima':'MP','Status':'STATUS','COMPANY':'COMPANY',
                    'RAW MATERIAL COD.':'CODIGO_MP'},
}

# =======================================================================================================================
# CARREGAR DATAFRAMES
# =======================================================================================================================
print('Carregando arquivos... \n')
print('Tempo de execução esperado: por volta de 16 min \n')

# DataFrame :: Horizonte (Período) de Otimização
df_periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']),
                         usecols=list(tp_dado_arquivos['periodos'].keys()),
                         dtype=tp_dado_arquivos['periodos'])

# Dataframe :: Portos existentes com códigos e correntes.
df_portos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['portos']),
                                  sheet_name = arquivos_primarios['portos_sn'],
                                  usecols = list(tp_dado_arquivos['portos'].keys()),
                                  dtype = tp_dado_arquivos['portos']).applymap(padronizar)

# Dataframe :: Cadastro Produtos
df_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['cadastro_produtos_sn'],
                                  usecols = list(tp_dado_arquivos['cadastro_produtos'].keys()),
                                  dtype = tp_dado_arquivos['cadastro_produtos'])

# DataFrame :: cadastro de matérias-primas :: filtro no tipo de material da tabela CADASTRO
cadastro_mp = df_produtos[(df_produtos['TIPO_MATERIAL'].str.split('-',expand=True)[0].str.strip() == 'MP')]

# Dataframe :: Agrupamento
agrupamento_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                                  sheet_name = arquivos_primarios['agrupamento_sn'],
                                  usecols = list(tp_dado_arquivos['agrupamento'].keys()),
                                  dtype = tp_dado_arquivos['agrupamento'])

# Dataframe :: Compras importadas :: importa todas as compras firmes IMPORTADAS ou NACIONALIZADAS
df_revisao_importada = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['compras_importadas']),
                                  sheet_name = arquivos_primarios['compras_importadas_sn'],
                                  usecols = list(tp_dado_arquivos['compras_importadas'].keys()),
                                  dtype = tp_dado_arquivos['compras_importadas']).applymap(padronizar)
df_revisao_importada = df_revisao_importada.rename(columns=rename_dataframes['df_revisao_importada'])

# DataFrame :: Compras nacionais :: importa todas as compras firmes NACIONAIS
df_revisao_nacional = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['compras_nacionais']),
                                    sheet_name = arquivos_primarios['compras_nacionais_sn'].split('.')[0],
                                    usecols = list(tp_dado_arquivos['compras_nacionais'].keys()),
                                    dtype = tp_dado_arquivos['compras_nacionais']).applymap(padronizar)
df_revisao_nacional = df_revisao_nacional.rename(columns=rename_dataframes['df_revisao_nacional'])

# Dataframe :: Custo de Reposição
custos_mp = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['custos_mp']),
                                  sheet_name = arquivos_primarios['custos_mp_sn'],
                                  usecols = list(tp_dado_arquivos['custos_mp'].keys()),
                                  dtype = tp_dado_arquivos['custos_mp'])
custos_mp = custos_mp.loc[custos_mp['CUSTO_REPOSICAO_MERCADO'] > 0.0,:].reset_index().drop(columns='index')

# Dataframe :: Demurrage
demurrage = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['demurrage']),
                                  sheet_name = arquivos_primarios['demurrage_sn'],
                                  dtype = tp_dado_arquivos['demurrage'])
ptax_demurrage = pd.read_excel(os.path.join(path + arquivos_primarios['demurrage']), sheet_name = 'PTAX',
                                  dtype =tp_dado_arquivos['ptax'])

# Dataframe :: Template Suprimento
validar_data_arquivo(os.path.join(cwd, path + arquivos_primarios['template_suprimento']))
template_suprimento = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_suprimento']),
                                  usecols = list(tp_dado_arquivos['template_suprimento'].keys()),
                                  dtype = tp_dado_arquivos['template_suprimento'])
wizard_suprimento_faixa = template_suprimento[['Unidade', 'Produto', 'Periodo']]


# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================
print('Carregando Plano de Compras...')

# 1. Definindo matérias-primas com fornecimento nacional
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
left_outer_join(df_revisao_importada,agrupamento_produtos,left_on='CODIGO_MP',right_on='COD_ESPECIFICO')
left_outer_join(df_revisao_importada, df_periodos, left_on = 'DT_REMESSA', right_on = 'PERIODO')
left_outer_join(df_revisao_importada, cadastro_mp, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM')

# 2.2. Nacional
id_vars = ['PORTO','PLANTA','MP','STATUS','COMPANY','CODIGO_MP']
df_revisao_nacional = df_revisao_nacional.melt(id_vars = id_vars, var_name = 'PROXY_PERIODO',
                                               value_name = 'BALANCE_TONS')
df_revisao_nacional['COMPANY'] = df_revisao_nacional['COMPANY'].replace(companies)
df_revisao_nacional['PORTO'] = df_revisao_nacional['PORTO'] + '-' + df_revisao_nacional['PLANTA']
left_outer_join(df_revisao_nacional, agrupamento_produtos, left_on = 'CODIGO_MP', right_on = 'COD_ESPECIFICO')
left_outer_join(df_revisao_nacional, df_periodos, left_on = 'PROXY_PERIODO', right_on = 'NOME_PERIODO')
left_outer_join(df_revisao_nacional, cadastro_mp, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM')

# 3. DataFrame de Compras Completo :: Importado + Nacional
cols = ['PORTO','PLANTA','MP','COMPANY','CODIGO_MP','COD_ESPECIFICO','CODIGO_AGRUPADO',
        'PERIODO','NOME_PERIODO','PRD-VCM','DESCRICAO','TIPO_MATERIAL','CATEGORIA','BALANCE_TONS']
df_revisao_importada = df_revisao_importada[cols]
df_revisao_nacional = df_revisao_nacional[cols]
df_revisao = pd.concat([df_revisao_importada,df_revisao_nacional])
df_revisao = df_revisao.reset_index().drop(columns='index')
left_outer_join(df_revisao, df_portos, left_on = 'PORTO', right_on = 'PORTO')

# Salvando um dataframe com o histórico da execução para log_futuro
exec_hist_df_revisao = df_revisao.copy()

print('Agrupando dados...')
df_revisao = df_revisao.groupby(by = ['NOME_PORTO_VCM','PRD-VCM','NOME_PERIODO'])['BALANCE_TONS'].sum().reset_index()
df_revisao['pk_right'] = df_revisao['NOME_PORTO_VCM'] + '-' + df_revisao['PRD-VCM'] + '-' + df_revisao['NOME_PERIODO']
print('Inserindo dados na estrutura topológica...')
template_suprimento['Suprimento Mínimo'] = 0.0
template_suprimento['Suprimento Máximo'] = 0.0
template_suprimento['pk_left'] = template_suprimento['Unidade'] + '-' + template_suprimento['Produto'] + '-' + template_suprimento['Periodo']
left_outer_join(template_suprimento,df_revisao, left_on = 'pk_left', right_on = 'pk_right')
print('\nAplicando premissas para compras firmes...')
print(' >> Horizonte Compras Importadas: M+0 até M+3')
print(' >> Horizonte Compras Nacionais: M+0 até M+1')
purchase_range = ['Mês000','Mês001','Mês002','Mês003']
purchase_range_nac = ['Mês000','Mês001']
mp_list_nac = list(mp_fornecimento_nacional['PRD-VCM-NAC'])
for i in tqdm(range(template_suprimento.shape[0]), desc = 'Processando...', unit = ' row'):
    # Caso 1: Está no horizonte de compra importada congelado e o fornecimento é importado
    if template_suprimento['Periodo'][i].split(' ')[0] in purchase_range \
       and template_suprimento['BALANCE_TONS'][i] > 0.0 \
       and template_suprimento['Unidade'][i][:3] == 'POR':
        template_suprimento['Suprimento Mínimo'][i] = template_suprimento['BALANCE_TONS'][i]
        template_suprimento['Suprimento Máximo'][i] = template_suprimento['Suprimento Mínimo'][i]
    
    # Caso 2: Não está no horizonte de fornecimento importado congelado e o fornecimento é importado
    elif template_suprimento['Periodo'][i].split(' ')[0] not in purchase_range \
       and template_suprimento['Unidade'][i][:3] == 'POR':
        template_suprimento['Suprimento Mínimo'][i] = template_suprimento['BALANCE_TONS'][i]
        template_suprimento['Suprimento Máximo'][i] = 100000.0
    
    # Caso 3: Para os casos de fornecimento nacional
    elif template_suprimento['Unidade'][i][:7] == 'FOR-NAC':
        # Caso 3.1: Suprimento mínimo supera o threshold de 10 kton
        if template_suprimento['BALANCE_TONS'][i] > 10000.0:
            template_suprimento['Suprimento Mínimo'][i] = template_suprimento['BALANCE_TONS'][i]
            template_suprimento['Suprimento Máximo'][i] = template_suprimento['Suprimento Mínimo'][i]
        # Caso 3.2: Por notar que a otimização sugeria com muita facilidade compras no período de 000 a 001,
        # uma nova premissa foi estabelecida em 29/05/2023 - Horizonte de Fornecimento Nacional Congelado
        elif template_suprimento['Periodo'][i].split(' ')[0] in purchase_range_nac:
            template_suprimento['Suprimento Mínimo'][i] = template_suprimento['BALANCE_TONS'][i]
            template_suprimento['Suprimento Máximo'][i] = template_suprimento['Suprimento Mínimo'][i]
        # Caso 3.3: Fornecimento Nacional está fora do Horizonte de Fornecimento Nacional Congelado
        elif template_suprimento['Produto'][i] in mp_list_nac:
            template_suprimento['Suprimento Mínimo'][i] = template_suprimento['BALANCE_TONS'][i]
            template_suprimento['Suprimento Máximo'][i] = 10000.0
        # Caso 3.4: Material não consta na lista de MPs adquiridas nacionalmente
        else:
            template_suprimento['Suprimento Mínimo'][i] = template_suprimento['BALANCE_TONS'][i]    
            template_suprimento['Suprimento Máximo'][i] = 0.0

columns = ['Unidade','Produto','Periodo','Suprimento Mínimo','Suprimento Máximo']
template_suprimento = template_suprimento[columns]
template_suprimento = template_suprimento.fillna(0.0)
template_suprimento = template_suprimento.round({'Suprimento Mínimo':2,'Suprimento Máximo':2})

# (30/04/2025) :: Linhas acima duplicadas do script supply.py

# CUSTOS DE FORNECIMENTO DE MATÉRIAS-PRIMAS
# =========================================
print('\n')
print('╔══════════════════════════════════════════════╗')
print('║ Iniciando atualização de Custos de Reposição ║')
print('╚══════════════════════════════════════════════╝')
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
last_updated_cost = last_updated_cost[['PERIODO','CD_PRODUTO_FTO','CODIGO_MOEDA','CUSTO_REPOSICAO_MERCADO']]
last_updated_cost = last_updated_cost.sort_values(by = 'PERIODO', ascending = False)
last_updated_cost = last_updated_cost.reset_index().drop(columns = 'index')
last_updated_cost['Custo VCM (BRL/ton)'] = np.nan
for i in range(last_updated_cost.shape[0]):
    if last_updated_cost['CODIGO_MOEDA'][i] == 'USD':
        last_updated_cost['Custo VCM (BRL/ton)'][i] = last_updated_cost['CUSTO_REPOSICAO_MERCADO'][i] * ptax_UF
    else:
        last_updated_cost['Custo VCM (BRL/ton)'][i] = last_updated_cost['CUSTO_REPOSICAO_MERCADO'][i]
last_updated_cost = last_updated_cost.drop_duplicates(subset=['CD_PRODUTO_FTO'], keep = 'first')

# (11/02/2025) Olhando primeiro o código específico, depois o agrupado.
# Utilizando o "Agrupamento" do cadastro de produtos.
agrupamento_produtos = agrupamento_produtos.drop_duplicates(subset=['COD_ESPECIFICO'])
agrupamento_produtos = agrupamento_produtos.merge(cadastro_mp,how = 'left', right_on = 'CODIGO_ITEM', left_on = 'CODIGO_AGRUPADO')

last_updated_cost = last_updated_cost.merge(agrupamento_produtos, how = 'left', right_on = 'COD_ESPECIFICO', left_on = 'CD_PRODUTO_FTO')
last_updated_cost['COD_ESPECIFICO'] = last_updated_cost['COD_ESPECIFICO'].fillna('0')
agrupamento_custo = last_updated_cost.loc[last_updated_cost['COD_ESPECIFICO']=='0']
agrupamento_custo = agrupamento_custo[['CD_PRODUTO_FTO','Custo VCM (BRL/ton)']]
agrupamento_custo = agrupamento_custo.merge(agrupamento_produtos, how = 'left', right_on = 'CODIGO_AGRUPADO', left_on = 'CD_PRODUTO_FTO')
agrupamento_custo['CODIGO_AGRUPADO'] = agrupamento_custo['CODIGO_AGRUPADO'].fillna('0')
agrupamento_custo = agrupamento_custo.loc[agrupamento_custo['CODIGO_AGRUPADO']!='0']
last_updated_cost = last_updated_cost.loc[last_updated_cost['CODIGO_AGRUPADO']!='0']
last_updated_cost = pd.concat([last_updated_cost,agrupamento_custo])
last_updated_cost = last_updated_cost[['Custo VCM (BRL/ton)','PRD-VCM','CD_PRODUTO_FTO']]
last_updated_cost = last_updated_cost.dropna().reset_index().drop(columns = 'index')
last_updated_cost = last_updated_cost[['PRD-VCM','Custo VCM (BRL/ton)']].rename(columns = {'Custo VCM (BRL/ton)':'LAST_UPDATED_COST','PRD-VCM':'ID'})

# (11/02/2025) Olhando primeiro o código específico, depois o agrupado.
tbDadoPrimarioCustoReposicao = custos_mp.merge(agrupamento_produtos, how = 'left', right_on = 'COD_ESPECIFICO', left_on = 'CD_PRODUTO_FTO')
tbDadoPrimarioCustoReposicao['COD_ESPECIFICO'] = tbDadoPrimarioCustoReposicao['COD_ESPECIFICO'].fillna('0')
agrupamento_custo = tbDadoPrimarioCustoReposicao.loc[tbDadoPrimarioCustoReposicao['COD_ESPECIFICO']=='0']
agrupamento_custo = agrupamento_custo[['DH_VIGOR','DH_REFERENCIA', 'Data Inicial', 'Data Final', 'CD_PRODUTO_FTO','DESCRICAO_ITEM',
                                    'CODIGO_ORGANIZACAO','CODIGO_MOEDA','PTAX_DIA_ANTERIOR','CUSTO_REPOSICAO_MERCADO','PERIODO','NOME_PERIODO']]
agrupamento_custo = agrupamento_custo.merge(agrupamento_produtos, how = 'left', right_on = 'CODIGO_AGRUPADO', left_on = 'CD_PRODUTO_FTO')
agrupamento_custo['CODIGO_AGRUPADO'] = agrupamento_custo['CODIGO_AGRUPADO'].fillna('0')
agrupamento_custo = agrupamento_custo.loc[agrupamento_custo['CODIGO_AGRUPADO']!='0']
tbDadoPrimarioCustoReposicao = tbDadoPrimarioCustoReposicao.loc[tbDadoPrimarioCustoReposicao['COD_ESPECIFICO']!='0']
custos_mp = pd.concat([tbDadoPrimarioCustoReposicao, agrupamento_custo])

# Criar regra para estabelecer períodos
# Utilizar LAST_UPDATED_COST caso False
custos_mp['Validar'] = (custos_mp['PERIODO'] >= custos_mp['Data Inicial']) & (custos_mp['PERIODO'] <= custos_mp['Data Final'])
custos_mp = custos_mp.loc[custos_mp.Validar == True]
custos_mp = custos_mp.reset_index().drop(columns = ['index','Validar','Data Inicial','Data Final'])
custos_mp = custos_mp.merge(agrupamento_produtos, how = 'left', right_on = 'CODIGO_ITEM', left_on = 'CD_PRODUTO_FTO')
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

demurrage = demurrage.merge(df_periodos, how = 'left', left_on = 'Periodo', right_on = 'PERIODO')
demurrage = demurrage.merge(df_portos, how = 'left', left_on = 'Porto', right_on = 'PORTO')
demurrage['ID-RIGHT'] = demurrage['NOME_PORTO_VCM'] + '-' + demurrage['NOME_PERIODO']
wizard_custo_suprimento_faixa['Custo MP BRL/ton'] = 0.0
wizard_custo_suprimento_faixa['Demurrage BRL/ton'] = 0.0
wizard_custo_suprimento_faixa = wizard_custo_suprimento_faixa.merge(agrupamento_produtos[['PRD-VCM','CATEGORIA']], how = 'left', left_on='Produto',right_on='PRD-VCM')
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
left_outer_join(wizard_suprimento_faixa, wizard_custo_suprimento_faixa, left_on = ['Unidade','Produto','Periodo'], right_on = ['Unidade','Produto','Periodo'])
wizard_suprimento_faixa = wizard_suprimento_faixa.fillna(0.0)
wizard_suprimento_faixa.to_excel(os.path.join(cwd,output_path + 'WIZARD_CUSTOS_FORNECIMENTO.xlsx'), sheet_name = 'CUSTO_PRODUTO', index = False)
print('Arquivo (Wizard_Custos_Fornecimento.xlsx) foi Atualizado com Sucesso!')
end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')