print('\n')
print('╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗')
print('║                                           ATUALIZACAO DE DADOS - VCM                                           ║')
print('║                                                >>  bind.py  <<                                                 ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ Criado por:    Isabela Nunes dos Santos        Data: 14/05/2025                                                ║')
print('║ Editado por:   Isabela Nunes dos Santos        Data: 21/05/2025                                                ║')
print('╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣')
print('║ CHANGELOG:                                                                                                     ║')
print('║ - v1.0.0 (21/05/2025): Criação da primeira versão do script unificado com edições estruturais nos arquivos     ║')
print('║                        de depara e dado primário.                                                              ║')
print('║                                                                                                                ║')
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
     'cadastro_produtos': 'depSKU.xlsx',
     'cadastro_produtos_sn':'CADASTRO',
     'cadastro_agrupamento':'AGRUPAMENTO',
     'portos': 'depUnidadesPortuarias.xlsx',
     'portos_sn': 'depUnidadesPortuarias',
     'compras_importadas':'iptComprasImportadas.xlsx',
     'compras_importadas_sn': 'iptComprasImportadas',
     'compras_nacionais': 'iptComprasNacionais.xlsx',
     'compras_nacionais_sn': 'iptComprasNacionais',
     'template_suprimento' : 'tmpSuprimento.xlsx',
     'demanda': 'iptDemandaIrrestrita.xlsx',
     'demanda_sn': 'Demanda',
     'unidades_expedicao': 'depUnidadesProdutivas.xlsx',
     'unidades_expedicao_sn': 'depUnidadesProdutivas',
     'unidades_terceiras': 'depUnidadesGerencias.xlsx',
     'unidades_terceiras_sn': 'depUnidadesGerencias',
     'template_demanda': 'tmpDemanda.xlsx',
     'supervisoes': 'depEstruturaComercial.xlsx',
     'supervisoes_sn': 'depEstruturaComercial',
     'dicionario': 'depDicionarioGenerico.xlsx',
     'dicionario_sn': 'depDicionarioGenerico',
     'dep_correntes': 'iptUpdateCorrentes.xlsx',
     'dep_correntes_sn': 'iptUpdateCorrentes',
     'template_limites': 'tmpDefinicaoLimites.csv',
     'template_correntes': 'tmpCorrentes.csv',
}

tp_dado_arquivos = {
     'periodos':{'NUMERO':np.int64,'PERIODO':'datetime64[ns]', 'NOME_PERIODO':str},
     'cadastro_produtos':{'PRD-VCM':str,'CODIGO_ITEM':str,'DESCRICAO':str,'TIPO_MATERIAL':str,'CATEGORIA':str},
     'cadastro_agrupamento':{'COD_ESPECIFICO':str,'DESCRICAO_ESPECIFICA':str,'CODIGO_AGRUPADO':str,
                                'AGRUPAMENTO_MP':str},
     'portos':{'NOME_PORTO_VCM':str,'NOME_AZ_PORTO_VCM':str,'PORTO':str,'UNIDADE':str,'CORRENTE':str},
     'correntes':{'NOME_PORTO_VCM':str,'PORTO':str,'UNIDADE':str,'CORRENTE':str},
     'compras_importadas':{'Porto':str,'Fábrica':str,'Matéria-prima':str,'Mês Entrega':'datetime64[ns]',
                   'BALANCE (TONS)':np.float32,'Status':str,'COMPANY':str,'RAW MATERIAL COD.':str},
     'compras_nacionais':{'Porto':str,'Fábrica':str,'Matéria-prima':str,'Status':str,'COMPANY':str,
                            'RAW MATERIAL COD.':str,'Mês000':np.float32,'Mês001':np.float32,'Mês002':np.float32,
                            'Mês003':np.float32,'Mês004':np.float32,'Mês005':np.float32,'Mês006':np.float32,
                            'Mês007':np.float32,'Mês008':np.float32,'Mês009':np.float32,'Mês010':np.float32,
                            'Mês011':np.float32,'Mês012':np.float32},
     'template_suprimento':{'Unidade':str, 'Produto':str, 'Periodo':str, 'Suprimento Mínimo':np.float64, 'Suprimento Máximo':np.float64},
     'demanda':{'PERIODO':'datetime64[ns]', 'DIRETORIA':str, 'GERENCIA':str, 'CONSULTORIA':str,
                'UNIDADE PRODUTORA':str, 'CULTURA':str, 'GRUPO PRODUTO':str, 'PRODUTO':str,
                'CODIGO PRODUTO':np.int64, 'RM_PREMIUM_DESCRIPTION_ENG':str, 'QUANTIDADE':np.int64, 'MP AGRUPADA':str},
     'unidades_expedicao':{'DEPOSITO':str, 'PLANTA':str, 'DESCRICAO_DEPOSITO':str, 'DESCRICAO_PLANTA':str, 'TIPO_UNIDADE':str,
                           'UNIDADE_ARMAZENAGEM_VCM':str, 'UP_MISTURADORA_VCM':str, 'UNIDADE_EXPEDICAO_VCM':str}, #esse tá BEM diferntes, vamos ver como se comporta no código (eyes)
     'unidades_terceiras':{'UNIDADE PRODUTORA':str, 'UNIDADE FATURAMENTO':str, 'GERENCIA':str, 'CONSULTORIA':str}, 
     'supervisoes':{'CHAVE':str, 'DIRETORIA':str, 'GERENCIA':str, 'CONSULTORIA':str, 'CENTROID':str, 'VCM':str, 'NOVA GERÊNCIA':str, 'BU':str, 'UF':str},
     'template_demanda':{'Unidade':str, 'Produto':str, 'Periodo':str, 'Demanda Mínima':np.float64, 'Demanda Máxima':np.float64},
     'dicionario':{'DE':str, 'PARA':str},
     'dep_correntes':{'ConjuntoCorrentes':str, 'Unidade-Origem':str, 'Unidade-Destino':str, 'Tipo':str},
     'template_limites':{'Unidade':str, 'Nivel Detalhe':str},
     'template_correntes':{'Unidade':str, 'Periodo':str, 'Produto':str, 'Limite':str, 'Ativo':str},
}	

rename_dataframes = {
    'df_periodos':{'NUMERO':'Numero','NOME_PERIODO':'Nome VCM', 'PERIODO':'Nome'},
    'df_revisao_importada':{'Porto':'PORTO','Fábrica':'PLANTA','Matéria-prima':'MP','Mês Entrega':'DT_REMESSA',
                   'BALANCE (TONS)':'BALANCE_TONS','Status':'STATUS','COMPANY':'COMPANY','RAW MATERIAL COD.':'CODIGO_MP'},
    'df_revisao_nacional':{'Porto':'PORTO','Fábrica':'PLANTA','Matéria-prima':'MP','Status':'STATUS','COMPANY':'COMPANY',
                            'RAW MATERIAL COD.':'CODIGO_MP'}
}

# =======================================================================================================================
# CARREGAR DATAFRAMES
# =======================================================================================================================
print('Carregando arquivos necessários... \n')

# DataFrame :: Horizonte (Período) de Otimização
df_periodos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['periodos']),
                            usecols=list(tp_dado_arquivos['periodos'].keys()),
                            dtype=tp_dado_arquivos['periodos'])
# applymap(padronizar) não aplicado por se tratar de dados com a estrutura final do VCM
df_periodos['pk_NOME_PERIODO'] = df_periodos['NOME_PERIODO'].str.split(' ', expand = True)[0]
id_periodos = df_periodos['NOME_PERIODO'].to_frame()

# DataFrame :: Portos
df_portos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['portos']),
                            sheet_name= arquivos_primarios['portos_sn'], 
                            usecols=list(tp_dado_arquivos['portos'].keys()),
                            dtype=tp_dado_arquivos['portos']).applymap(padronizar)
id_portos = df_portos.drop(columns=['PORTO']).drop_duplicates()

# DataFrame :: Correntes
df_correntes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['portos']),
                            sheet_name= arquivos_primarios['portos_sn'], 
                            usecols=list(tp_dado_arquivos['correntes'].keys()),
                            dtype=tp_dado_arquivos['correntes']).applymap(padronizar)

# DataFrame :: Update Correntes
dep_correntes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['dep_correntes']),
                            sheet_name= arquivos_primarios['dep_correntes_sn'], 
                            usecols=list(tp_dado_arquivos['dep_correntes'].keys()),
                            dtype=tp_dado_arquivos['dep_correntes']).applymap(padronizar)

# DataFrame :: Cadastro de Produtos
df_produtos = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                            sheet_name = arquivos_primarios['cadastro_produtos_sn'],
                            usecols = list(tp_dado_arquivos['cadastro_produtos'].keys()),
                            dtype = tp_dado_arquivos['cadastro_produtos'])

# DataFrame :: cadastro de matérias-primas :: filtro no tipo de material da tabela CADASTRO
cadastro_mp = df_produtos[(df_produtos['TIPO_MATERIAL'].str.split('-',expand=True)[0].str.strip() == 'MP')]

# DataFrame :: cadastro de produto final :: filtro no tipo de material da tabela CADASTRO
pf_cadastrada = df_produtos[(df_produtos['TIPO_MATERIAL'].str.split('-',expand=True)[0].str.strip() == 'PF')]

# DataFramse :: Agrupamento
df_agrupamento = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['cadastro_produtos']),
                            sheet_name = arquivos_primarios['cadastro_agrupamento'],
                            usecols = list(tp_dado_arquivos['cadastro_agrupamento'].keys()),
                            dtype = tp_dado_arquivos['cadastro_agrupamento'])
depara_pf_demanda = df_agrupamento[['CODIGO_AGRUPADO','COD_ESPECIFICO']]

proxy_agrupamento = df_produtos[['CODIGO_ITEM','DESCRICAO']]
proxy_agrupamento = proxy_agrupamento.rename(columns={'CODIGO_ITEM':'COD_ESPECIFICO','DESCRICAO':'DESCRICAO_ESPECIFICA'})
proxy_agrupamento['CODIGO_AGRUPADO'] = proxy_agrupamento['COD_ESPECIFICO']
proxy_agrupamento['AGRUPAMENTO_MP'] = proxy_agrupamento['DESCRICAO_ESPECIFICA']
agrupamento_produtos = pd.concat([df_agrupamento,proxy_agrupamento])
agrupamento_produtos = agrupamento_produtos.drop_duplicates(subset = 'COD_ESPECIFICO')

# DataFrame :: Compras Importadas
df_compras_importadas = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['compras_importadas']),
                            sheet_name = arquivos_primarios['compras_importadas_sn'],
                            usecols = list(tp_dado_arquivos['compras_importadas'].keys()),
                            dtype = tp_dado_arquivos['compras_importadas'])

df_compras_importadas = df_compras_importadas.rename(columns=rename_dataframes['df_revisao_importada'])

# DataFrame :: Compras Nacionais
df_compras_nacionais = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['compras_nacionais']),
                            sheet_name = arquivos_primarios['compras_nacionais_sn'],
                            usecols = list(tp_dado_arquivos['compras_nacionais'].keys()),
                            dtype = tp_dado_arquivos['compras_nacionais']).applymap(padronizar)
df_compras_nacionais = df_compras_nacionais.rename(columns=rename_dataframes['df_revisao_nacional'])

# DataFrame :: Template Suprimento
template_suprimento = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_suprimento']),
                            usecols = list(tp_dado_arquivos['template_suprimento'].keys()),
                            dtype = tp_dado_arquivos['template_suprimento'])

# DataFrame :: 
df_demanda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['demanda']),
                           sheet_name = arquivos_primarios['demanda_sn'],
                           usecols = list(tp_dado_arquivos['demanda'].keys()),
                           dtype = tp_dado_arquivos['demanda'])

# DataFrame :: 
df_unidades = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_expedicao']),
                            sheet_name = arquivos_primarios['unidades_expedicao_sn'],
                            usecols = list(tp_dado_arquivos['unidades_expedicao'].keys()),
                            dtype = tp_dado_arquivos['unidades_expedicao'])

# DataFrame :: 
df_supervisoes = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['supervisoes']),
                               sheet_name = arquivos_primarios['supervisoes_sn'],
                               usecols = list(tp_dado_arquivos['supervisoes'].keys()),
                               dtype = tp_dado_arquivos['supervisoes'])

# DataFrame :: 
df_terceiras = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['unidades_terceiras']),
                             sheet_name = arquivos_primarios['unidades_terceiras_sn'],
                             usecols = list(tp_dado_arquivos['unidades_terceiras'].keys()),
                             dtype = tp_dado_arquivos['unidades_terceiras'])

# DataFrame :: Dicionário Genérico
df_dicionario = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['dicionario']),
                              sheet_name = arquivos_primarios['dicionario_sn'],
                              usecols = list(tp_dado_arquivos['dicionario'].keys()),
                              dtype = tp_dado_arquivos['dicionario'])

# DataFrame :: Template Demanda
template_demanda = pd.read_excel(os.path.join(cwd, path + arquivos_primarios['template_demanda']),
                            usecols = list(tp_dado_arquivos['template_demanda'].keys()),
                            dtype = tp_dado_arquivos['template_demanda'])

# DataFrame :: Template Definição Limites
template_limites = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_limites']),\
                              delimiter = ';', encoding = 'utf-8')

# DataFrame :: Template Correntes
template_correntes = pd.read_csv(os.path.join(cwd, path + arquivos_primarios['template_correntes']),\
                              delimiter = ';', encoding = 'utf-8')


# =======================================================================================================================
# EXECUÇÃO DE SCRIPTS
# =======================================================================================================================

print('\n')
print('╔════════════════════════════════════════════════════════════════════╗')
print('║                      >>  PLANO DE COMPRAS  <<                      ║')
print('╠════════════════════════════════════════════════════════════════════╣')
print('║ # WIZARD_SUPRIMENTO_FAIXA :: Plano de Compras Firmes para VCM      ║')
print('╚════════════════════════════════════════════════════════════════════╝')
print('\n')
print('Iniciando...')

# 1. Definindo matérias-primas com fornecimento nacional

mp_fornecimento_nacional = cadastro_mp[(cadastro_mp['TIPO_MATERIAL'] == 'MP - COMPRAS')]['PRD-VCM']
mp_fornecimento_nacional = mp_fornecimento_nacional.reset_index().drop(columns='index')
mp_fornecimento_nacional = mp_fornecimento_nacional.rename(columns={'PRD-VCM':'PRD-VCM-NAC'})
mp_fornecimento_nacional = mp_fornecimento_nacional.drop_duplicates()

# 2. Tratando arquivos de plano de compras IMPORTADO e NACIONAL
# 2.1. Importado
companies = {'FTO':'E600','FH':'E900','SAL':'E890','CMISS':'E890','FHG':'E900','ECFTO':'E600','SFT':'E890'}
df_compras_importadas['COMPANY'] = df_compras_importadas['COMPANY'].replace(companies)
df_compras_importadas = df_compras_importadas[(df_compras_importadas['STATUS'] == 'COMPRADO')]
df_compras_importadas['DT_REMESSA'] = df_compras_importadas['DT_REMESSA'] - pd.offsets.MonthBegin(1)
left_outer_join(df_compras_importadas,agrupamento_produtos,left_on='CODIGO_MP',right_on='COD_ESPECIFICO')
left_outer_join(df_compras_importadas, df_periodos, left_on = 'DT_REMESSA', right_on = 'PERIODO')
left_outer_join(df_compras_importadas, cadastro_mp, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM')

# 2.2. Nacional
id_vars = ['PORTO','PLANTA','MP','STATUS','COMPANY','CODIGO_MP']
df_revisao_nacional = df_compras_nacionais.melt(id_vars = id_vars, var_name = 'PROXY_PERIODO',
                                               value_name = 'BALANCE_TONS')
df_revisao_nacional['COMPANY'] = df_revisao_nacional['COMPANY'].replace(companies)
df_revisao_nacional['PORTO'] = df_revisao_nacional['PORTO'] + '-' + df_revisao_nacional['PLANTA']
left_outer_join(df_revisao_nacional, agrupamento_produtos, left_on = 'CODIGO_MP', right_on = 'COD_ESPECIFICO')
left_outer_join(df_revisao_nacional, df_periodos, left_on = 'PROXY_PERIODO', right_on = 'pk_NOME_PERIODO')
left_outer_join(df_revisao_nacional, cadastro_mp, left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM')

# 3. DataFrame de Compras Completo :: Importado + Nacional
cols = ['PORTO','PLANTA','MP','COMPANY','CODIGO_MP','COD_ESPECIFICO','CODIGO_AGRUPADO','PERIODO','NOME_PERIODO','PRD-VCM','DESCRICAO','TIPO_MATERIAL','CATEGORIA','BALANCE_TONS']
df_revisao_importada = df_compras_importadas[cols]
df_revisao_nacional = df_revisao_nacional[cols]
df_revisao = pd.concat([df_revisao_importada,df_revisao_nacional])
df_revisao = df_revisao.reset_index().drop(columns='index')
left_outer_join(df_revisao, df_portos, left_on = 'PORTO', right_on = 'PORTO')

# Tá no arquivo de supply, faz sentido ter isso em bind?
# # Salvando um dataframe com o histórico da execução para log_futuro
# exec_hist_df_revisao = df_revisao.copy()
# Linhas acima pegas em supply.py 

df_revisao['Origem-Destino'] = ''
for j in range(df_revisao.shape[0]):
    df_revisao['Origem-Destino'][j] = df_revisao['PORTO'][j] + '-' + df_revisao['PLANTA'][j]
id_produtos = mp_fornecimento_nacional['PRD-VCM-NAC'].to_frame().rename(columns={'PRD-VCM-NAC':'PRD-VCM'})
df_correntes['ID_correntes'] = ''
for z in range(df_correntes.shape[0]):
    df_correntes['ID_correntes'][z] = df_correntes['PORTO'][z] + '-' + df_correntes['UNIDADE'][z]
id_correntes = df_correntes['CORRENTE']

print('Carregando estrutura topológica...')
template_suprimento['ID'] = template_suprimento['Unidade'] + '-' + template_suprimento['Produto'] + '-' + template_suprimento['Periodo']
# Criando o output WIZARD_SUPRIMENTO_FAIXA
wsf_query = pd.DataFrame(columns=['Unidade','Produto','Periodo','Suprimento Mínimo','Suprimento Máximo'])
wsf_query['Unidade'] = id_portos['NOME_PORTO_VCM']
wsf_query = wsf_query.merge(id_periodos, how='cross')
wsf_query['Periodo'] = wsf_query['NOME_PERIODO']
wsf_query = wsf_query.drop(columns='NOME_PERIODO')
wsf_query = wsf_query.merge(id_produtos,how='cross')
wsf_query['Produto'] = wsf_query['PRD-VCM']
wsf_query = wsf_query.drop(columns='PRD-VCM')
df_revisao['ID'] = df_revisao['PORTO'] + '-' + df_revisao['PRD-VCM'] + '-' + df_revisao['NOME_PERIODO']

# MERCADOS CONSUMIDORES
# =====================
# Está seção dedica-se ao ETL para a criação dos WIZARDS de MERCADOS CONSUMIDORES

df_agrupamento = df_agrupamento.merge(pf_cadastrada, how = 'left', left_on = 'CODIGO_AGRUPADO', right_on = 'CODIGO_ITEM')
df_agrupamento = df_agrupamento[['CODIGO_AGRUPADO','DESCRICAO_ESPECIFICA','PRD-VCM','COD_ESPECIFICO']]
df_agrupamento = df_agrupamento.dropna(subset = ['PRD-VCM'])
df_agrupamento = df_agrupamento.drop_duplicates(subset = ['CODIGO_AGRUPADO'])
df_agrupamento = df_agrupamento.reset_index()
df_agrupamento = df_agrupamento.drop(columns = ['index'])
id_produtos_mc = df_agrupamento.copy()
id_produtos_mc = id_produtos_mc['PRD-VCM'].to_frame()

df_supervisoes['ID'] = df_supervisoes['GERENCIA'] + '-' + df_supervisoes['CONSULTORIA']
id_mercados_consumidores = df_supervisoes
id_mercados_consumidores = id_mercados_consumidores['VCM'].to_frame().rename({'VCM':'ID MC'})
# RENOMEANDO O NOVO ARQUIVO DE DEMANDA IRRESTRITA PARA OS HEADERS ANTIGOS
rename_cols = {'CODIGO PRODUTO':'PRODUTO ID','QUANTIDADE':'VOLUME',
               'GERENCIA':'REGIONAL','CONSULTORIA':'SUPERVISAO', 'PERIODO':'PERIODO'}
df_demanda = df_demanda.rename(columns = rename_cols)
# SEPARAÇÃO DA DEMANDA POR EMPRESA
df_demanda = df_demanda.loc[df_demanda['PRODUTO ID'].notnull(),:]
df_demanda['PRODUTO ID'] = df_demanda['PRODUTO ID'].astype(np.int64)
depara_pf_demanda['COD_ESPECIFICO'] = depara_pf_demanda['COD_ESPECIFICO'].astype(np.int64)
df_demanda = df_demanda.merge(depara_pf_demanda, how = 'left', left_on = 'PRODUTO ID', right_on = 'COD_ESPECIFICO')
df_demanda = df_demanda.dropna(subset = ['CODIGO_AGRUPADO'])
df_demanda['Código Agrupado'] = df_demanda['CODIGO_AGRUPADO'].astype(np.int64)
df_demanda = df_demanda.drop(columns = ['PRODUTO ID','COD_ESPECIFICO'])
df_demanda = df_demanda.rename(columns = {'CODIGO_AGRUPADO':'PRODUTO ID'})
df_demanda['PRODUTO ID'] = df_demanda['PRODUTO ID'].astype('string')
# Criar uma lista de unidades terceiras para checar no arquivo de demanda
unique = df_terceiras['UNIDADE PRODUTORA'].drop_duplicates().to_list()
df_demanda['proxy.Faturamento'] = df_demanda['UNIDADE PRODUTORA'].apply(lambda x: x if x not in unique else np.NaN)

# Separar o arquivo de demanda com base na existência ou não na lista "unique"
demanda_unidade_standard = df_demanda.loc[df_demanda['proxy.Faturamento'].notna(),:].reset_index().drop(columns='index')
demanda_unidade_terceira = df_demanda.loc[df_demanda['proxy.Faturamento'].isna(),:].reset_index().drop(columns='index')
# Para o primeiro caso, a unidade de faturamento é a própria unidade produtora
demanda_unidade_standard['UNIDADE FATURAMENTO'] = demanda_unidade_standard['proxy.Faturamento']
# Recriar a lista agora apenas com as consultorias relevantes para determinar a UNIDADE FATURAMENTO
unique = df_terceiras.loc[df_terceiras['CONSULTORIA'].notna(),:]['GERENCIA'].drop_duplicates().to_list()
demanda_unidade_terceira['proxy.Supervisao'] = demanda_unidade_terceira['REGIONAL'].apply(lambda x: np.NaN if x not in unique else x)
demanda_unidade_terceira_na = demanda_unidade_terceira.loc[demanda_unidade_terceira['proxy.Supervisao'].isna(),:].reset_index().drop(columns='index')
demanda_unidade_terceira_notna = demanda_unidade_terceira.loc[demanda_unidade_terceira['proxy.Supervisao'].notna(),:].reset_index().drop(columns='index')
proxy = df_terceiras.loc[df_terceiras.CONSULTORIA.isna(),:].reset_index().drop(columns='index')
demanda_unidade_terceira_na = demanda_unidade_terceira_na.merge(proxy, how = 'left', left_on = ['UNIDADE PRODUTORA','REGIONAL'], right_on = ['UNIDADE PRODUTORA','GERENCIA'])
proxy = df_terceiras.loc[df_terceiras.CONSULTORIA.notna(),:].reset_index().drop(columns='index')
demanda_unidade_terceira_notna = demanda_unidade_terceira_notna.merge(proxy, how = 'left', left_on = ['UNIDADE PRODUTORA','REGIONAL','SUPERVISAO'],
                                                                      right_on = ['UNIDADE PRODUTORA','GERENCIA','CONSULTORIA'])
demanda = pd.concat([demanda_unidade_standard, demanda_unidade_terceira_na, demanda_unidade_terceira_notna])
demanda = demanda.merge(df_dicionario, how='left', left_on='UNIDADE PRODUTORA', right_on='DE')
demanda = demanda.merge(df_unidades, how = 'left', left_on = 'PARA', right_on = 'PLANTA')
demanda = demanda.merge(df_agrupamento, how = 'left', left_on = 'PRODUTO ID', right_on = 'CODIGO_AGRUPADO')
demanda = demanda[['REGIONAL','SUPERVISAO','VOLUME','PERIODO','UNIDADE_EXPEDICAO_VCM','PRD-VCM']]
demanda = demanda.dropna(subset = ['UNIDADE_EXPEDICAO_VCM','PRD-VCM'])
demanda['Regional - Supervisão'] = demanda['REGIONAL'] + '-' + demanda['SUPERVISAO']
demanda = demanda.merge(df_supervisoes, how = 'left', left_on = 'Regional - Supervisão', right_on = 'ID')
demanda = demanda[['PERIODO','PRD-VCM','UNIDADE_EXPEDICAO_VCM','VCM','VOLUME']]
demanda = demanda.merge(df_periodos, how = 'left', left_on = 'PERIODO', right_on = 'PERIODO')
demanda['ID Origem-Destino'] = demanda['UNIDADE_EXPEDICAO_VCM'] + '-' + demanda['VCM']
demanda = demanda.dropna(subset = ['PRD-VCM'])

##########################################################
##########################################################

# AMARRAÇÃO DAS CORRENTES DE CONSUMO
# ==================================
dep_correntes['ID'] = dep_correntes['Unidade-Origem'] + '-' + dep_correntes['Unidade-Destino']
demanda = demanda.merge(dep_correntes, how = 'left', left_on = 'ID Origem-Destino', right_on = 'ID')
demanda_corrente_agrupada = demanda.groupby(['ConjuntoCorrentes','NOME_PERIODO','PRD-VCM'])['VOLUME'].sum().reset_index()
demanda_corrente_agrupada = demanda_corrente_agrupada.rename(columns={'ConjuntoCorrentes':'Unidade','NOME_PERIODO':'Período','PRD-VCM':'Produto','VOLUME':'Limite'})
demanda_corrente_agrupada['Ativo'] = True

# AMARRAÇÃO DAS CORRENTES DE FORNECIMENTO
# =======================================
df_revisao_correntes_grouped = df_revisao.groupby(['CORRENTE','NOME_PERIODO','PRD-VCM'])['BALANCE_TONS'].sum().reset_index()
wizard_suprimento_amarracao = df_revisao_correntes_grouped
wizard_suprimento_amarracao = wizard_suprimento_amarracao.rename(columns={'CORRENTE':'Unidade','NOME_PERIODO)':'Período', 'PRD-VCM':'Produto','BALANCE_TONS':'Limite'})
wizard_suprimento_amarracao['Ativo'] = True
wizard_amarracao = pd.concat([demanda_corrente_agrupada,wizard_suprimento_amarracao])
wizard_amarracao['ID-RIGHT'] = wizard_amarracao['Unidade'] + wizard_amarracao['Período'] + wizard_amarracao['Produto']

# ATIVAÇÃO DO DETALHAMENTO POR PRODUTO
# ====================================
# Cria uma ativação por produto e por corrente
aux_wizard_amarracao = wizard_amarracao[['Unidade','Ativo']]
aux_wizard_amarracao = aux_wizard_amarracao.drop_duplicates()
template_limites = template_limites.merge(aux_wizard_amarracao, how = 'left', left_on = 'Unidade', right_on = 'Unidade')
template_limites.fillna(False)
for i in tqdm(range(template_limites.shape[0])):
    if template_limites['Ativo'][i] == True:
       template_limites['Nivel Detalhe'][i] = 'Detalhado por Produto'
    else:
        template_limites['Nivel Detalhe'][i] = template_limites['Nivel Detalhe'][i]

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
template_correntes = template_correntes.merge(wizard_amarracao, how = 'left', right_on = 'ID-RIGHT', left_on = 'ID-LEFT')
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
template_correntes.to_csv(os.path.join(cwd,output_path + 'Wizard de Limites.csv'),\
                  index = False, encoding = 'utf-8-sig', sep = ';')

print('Wizard de Limites :: Atualizado com Sucesso!')
end_time = time.time()
print(f'Tempo de Execução: {round(end_time - start_time,2)} segundos')